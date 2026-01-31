(ns clj-simple-stats.core
  (:require
   [clj-simple-stats.analyzer :as analyzer]
   [clj-simple-stats.dashboard :as dashboard]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [ring.middleware.cookies :as cookies]
   [ring.util.response :as response])
  (:import
   [java.io File]
   [java.lang AutoCloseable]
   [java.nio.file Files StandardCopyOption]
   [java.sql DriverManager]
   [java.time LocalDate LocalTime LocalDateTime ZoneId]
   [java.time.format DateTimeFormatter]
   [java.util ArrayList Random UUID]
   [java.util.concurrent LinkedBlockingQueue ScheduledFuture ScheduledThreadPoolExecutor ThreadFactory TimeUnit]
   [java.util.concurrent.locks ReentrantLock]
   [org.duckdb DuckDBConnection]))

(def ^:private ^ZoneId UTC
  (ZoneId/of "UTC"))

(def ^:private default-db-path
  "clj_simple_stats.duckdb")

(def ^:private default-uri
  "/stats")

(def ^LinkedBlockingQueue queue
  (LinkedBlockingQueue.))

(def ^ReentrantLock db-lock
  (ReentrantLock.))

(defmacro with-lock [lock & body]
  `(let [lock# ^ReentrantLock ~lock]
     (.lock lock#)
     (try
       (do
         ~@body)
       (finally
         (.unlock lock#)))))

(defmacro log-verbose [& msgs]
  #_`(println ~@msgs))

(defmacro log [& msgs]
  `(println ~@msgs))

(defn conn-path [^DuckDBConnection conn]
  (-> conn .getMetaData .getURL (str/replace #"^jdbc:duckdb:" "")))

(defn connect ^DuckDBConnection [db-path]
  (log-verbose "Opening" db-path)
  (DriverManager/getConnection (str "jdbc:duckdb:" db-path)))

(defn init-db! [^DuckDBConnection conn]
  (log "Initializing" (conn-path conn))
  (with-open [stmt (.createStatement conn)]
    (.execute stmt
      "CREATE TABLE IF NOT EXISTS version (version INTEGER)")
    (.execute stmt
      "INSERT INTO version VALUES (2)")
    (.execute stmt
      "CREATE TYPE IF NOT EXISTS agent_type_t AS ENUM ('feed', 'bot', 'browser')")
    (.execute stmt
      "CREATE TYPE IF NOT EXISTS agent_os_t AS ENUM ('Android', 'Windows', 'iOS', 'macOS', 'Linux')")
    (.execute stmt
      "CREATE TABLE IF NOT EXISTS stats (
         date       DATE,
         time       TIME,
         path       VARCHAR,
         query      VARCHAR,
         referrer   VARCHAR,
         type       agent_type_t,
         agent      VARCHAR,
         os         agent_os_t,
         ref_domain VARCHAR,
         mult       INTEGER,
         uniq       UUID
       )")))

(defn db-version ^long [^DuckDBConnection conn]
  (or
    (with-open [stmt (.createStatement conn)
                rs   (.executeQuery stmt "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'version'")]
      (when (and (.next rs) (pos? (.getLong rs 1)))
        (with-open [stmt' (.createStatement conn)
                    rs'   (.executeQuery stmt' "SELECT version FROM version")]
          (when (.next rs')
            (.getLong rs' 1)))))
    1))

(defn migrate-1->2! [^String db-path]
  (log "Migrating" db-path "to version 2")
  (let [tmp-file (io/file
                   (or (.getParentFile (io/file db-path)) (io/file "."))
                   (str "clj_simple_stats_migrate_" (-> (Random.) .nextLong Long/toUnsignedString) ".duckdb"))]
    (with-open [conn (connect (.getPath tmp-file))]
      (init-db! conn)
      (with-open [stmt (.createStatement conn)]
        (.execute stmt (str "ATTACH '" db-path "' AS olddb (READ_ONLY)"))
        (.execute stmt
          "INSERT INTO stats
             SELECT date, time, path, query, referrer, type, agent, os, ref_domain, mult, uniq
             FROM olddb.stats")))
    (Files/move
      (.toPath tmp-file)
      (.toPath (io/file db-path))
      (into-array [StandardCopyOption/REPLACE_EXISTING StandardCopyOption/ATOMIC_MOVE]))
    (log "Migration to version 2 complete")))

(defn check-db [db-path]
  (if-not (File/.exists (io/file db-path))
    (with-open [conn (connect db-path)]
      (init-db! conn))
    (let [v (with-open [conn (connect db-path)]
              (db-version conn))]
      (when (<= v 1)
        (migrate-1->2! db-path)))))

(def ^:private *worker-pool
  (atom nil))

(def ^:private *worker-task
  (atom nil))

(defmacro with-conn [[sym db-path] & body]
  (let [sym (vary-meta sym assoc :tag 'DuckDBConnection)]
    `(with-lock db-lock
       (with-open [~sym (connect ~db-path)]
         ~@body))))

(defn- content-type [resp]
  ((some-fn
     #(get % "Content-Type")
     #(get % "content-type")
     #(get % "Content-type"))
   (:headers resp)))

(defn- loggable? [req resp]
  (let [status (:status resp 200)
        mime   (content-type resp)]
    (and
      (= 200 status)
      (or
        (some-> mime (str/starts-with? "text/html"))
        (some-> mime (str/starts-with? "application/atom+xml"))
        (some-> mime (str/starts-with? "application/rss+xml"))))))

(defn- maybe-schedule-line! [req resp]
  (when (loggable? req resp)
    (let [now  (LocalDateTime/now UTC)
          mime (content-type resp)
          line {:date       (-> now .toLocalDate)
                :time       (-> now .toLocalTime (.withNano 0))
                :path       (:uri req)
                :query      (:query-string req)
                :ip         (or
                              (get (:headers req) "x-forwarded-for")
                              (:remote-addr req))
                :user-agent (get (:headers req) "user-agent")
                :referrer   (get (:headers req) "referer")
                :type       (cond
                              (some-> mime (str/starts-with? "application/atom+xml")) "feed"
                              (some-> mime (str/starts-with? "application/rss+xml"))  "feed")}]
      (.add queue line))))

(defn- insert-lines! [db-path lines]
  (log-verbose "Inserting" (count lines) "lines to" db-path)
  (with-open [conn (connect db-path)
              apnd (.createAppender conn DuckDBConnection/DEFAULT_SCHEMA "stats")]
    (doseq [lines (partition-all 1000 lines)]
      (doseq [line lines
              :let [line' (analyzer/analyze line)]]
        (.beginRow apnd)
        (.append apnd ^LocalDate (:date line'))
        (.append apnd ^LocalTime (:time line'))
        (.append apnd ^String    (:path line'))
        (.append apnd ^String    (:query line'))
        (.append apnd ^String    (:referrer line'))
        (.append apnd ^String    (:type line'))
        (.append apnd ^String    (:agent line'))
        (.append apnd ^String    (:os line'))
        (.append apnd ^String    (:ref-domain line'))
        (.append apnd            (int (:mult line')))
        (.append apnd ^UUID      (:uniq line'))
        (.endRow apnd))
      (.flush apnd)))
  nil)

(defn- maybe-shutdown-worker! []
  (when-some [task (first (reset-vals! *worker-task nil))]
    (.cancel ^ScheduledFuture task false)
    (log-verbose "Shut down worker"))
  (when-some [pool (first (reset-vals! *worker-pool nil))]
    (.shutdown ^ScheduledThreadPoolExecutor pool)))

(defn start-worker! [db-path]
  (log-verbose "Starting worker for" db-path)
  (let [pool (ScheduledThreadPoolExecutor. 1
               (reify ThreadFactory
                 (newThread [_ r]
                   (doto (Thread. r)
                     (.setDaemon true)
                     (.setName "clj-simple-stats.core/worker")))))
        task (fn []
               (try
                 (let [buf (ArrayList.)]
                   (.drainTo queue buf)
                   (when-not (.isEmpty buf)
                     (insert-lines! db-path buf)))
                 (catch Exception e
                   (log e))))]
    (reset! *worker-pool pool)
    (reset! *worker-task (.scheduleAtFixedRate pool ^Runnable task 1 1 TimeUnit/MINUTES))))

(defn wrap-collect-stats
  ([handler]
   (wrap-collect-stats handler {}))
  ([handler {:keys [db-path cookie-name]
             :or {db-path     default-db-path
                  cookie-name "stats_id"}}]
   (maybe-shutdown-worker!)
   (check-db db-path)
   (start-worker! db-path)
   (fn [req]
     (let [resp       (handler req)
           has-cookie (some-> req cookies/cookies-request :cookies (get cookie-name))]
       (maybe-schedule-line! req resp)
       (cond-> resp
         has-cookie (update :cookies assoc cookie-name {:value "" :max-age 0 :path "/"})
         has-cookie (cookies/cookies-response))))))

(defn render-stats
  ([req]
   (render-stats {} req))
  ([{:keys [db-path] :or {db-path default-db-path}} req]
   (with-conn [conn db-path]
     (dashboard/page conn req))))

(defn wrap-render-stats
  ([handler]
   (wrap-render-stats handler {}))
  ([handler {:keys [uri dash-perms-fn]
             :or {uri default-uri
                  dash-perms-fn (fn [_] true)}
             :as opts}]
   (fn [req]
     (cond
       (= uri (:uri req))
       (if (dash-perms-fn req)
         (render-stats opts req)
         {:status  401
          :headers {"content-type" "text/plain"}
          :body    "Unauthorized"})

       (= (str uri "/favicon.ico") (:uri req))
       (response/resource-response "clj_simple_stats/favicon.ico")

       :else
       (handler req)))))

(defn wrap-stats
  ([handler]
   (wrap-stats handler {}))
  ([handler opts]
   (-> handler
     (wrap-collect-stats opts)
     (wrap-render-stats opts))))

(defn before-ns-unload []
  (maybe-shutdown-worker!))
