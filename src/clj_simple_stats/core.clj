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

(def ^:private rollup-depth
  "How many values per (date, dim) rollup_daily keeps;
   the rest are merged into rollup-sentinel"
  20)

(def ^:private rollup-sentinel
  dashboard/rollup-sentinel)

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

(defmacro with-conn [[sym db-path] & body]
  (let [sym (vary-meta sym assoc :tag 'DuckDBConnection)]
    `(with-lock db-lock
       (with-open [~sym (connect ~db-path)]
         ~@body))))

(def ^:private conn-ttl-ms
  (* 5 60 1000))

(def ^:private cache-lock
  (ReentrantLock.))

(def ^:private *conn-cache
  "nil | {:conn     <master copy of shared conn>
          :db-path  <path>
          :deadline <millis> | nil}"
  (atom nil))

(defn- acquire-conn
  "Returns duplicate of cached conn. Initializes cache if needed.
   Returned value must be closed.

   Idea here is that dashboard keeps conn open for 5 minutes after last access (reset-ttl?),
   insert-lines! and rollup! can reuse open connection but don't prolong its lifetime"
  ^DuckDBConnection [db-path & {:keys [reset-ttl?]}]
  (with-lock cache-lock
    (when (nil? @*conn-cache)
      (reset! *conn-cache {:conn (connect db-path), :db-path db-path, :deadline nil}))
    (if (= db-path (:db-path @*conn-cache))
      (let [cache @*conn-cache]
        (when reset-ttl?
          (swap! *conn-cache assoc :deadline (+ (System/currentTimeMillis) conn-ttl-ms)))
        (DuckDBConnection/.duplicate (:conn cache)))
      ;; second database in one process: don't disturb the cache
      (connect db-path))))

(defn- close-cached-conn! []
  (with-lock cache-lock
    (when-some [cache (first (reset-vals! *conn-cache nil))]
      (log-verbose "Closing" (:db-path cache))
      (AutoCloseable/.close (:conn cache)))))

(defn- close-cached-conn-if-expired! []
  (with-lock cache-lock
    (when-some [{:keys [deadline]} @*conn-cache]
      (when (or (nil? deadline) (< ^long deadline (System/currentTimeMillis)))
        (close-cached-conn!)))))

(defn init-db! [^DuckDBConnection conn]
  (log "Initializing" (conn-path conn))
  (with-open [stmt (.createStatement conn)]
    (.execute stmt
      "CREATE TABLE IF NOT EXISTS version (version INTEGER)")
    (.execute stmt
      "INSERT INTO version VALUES (4)")
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
       )")
    (.execute stmt
      "CREATE TYPE IF NOT EXISTS dim_t AS ENUM ('feed', 'bot', 'browser', 'path', 'query', 'ref_domain')")
    (.execute stmt
      ;; Per-day aggregates of stats for all days <= rollup_state.last_date.
      ;; For dims feed/bot/browser, value is agent (possibly NULL).
      ;; For dims path/query/ref_domain, value is that column.
      ;; Only the top 20 (rollup-depth) values per (date, dim) are kept;
      ;; the remainder is merged into one rollup-sentinel row.
      ;; value NULL -> attribute unknown: counted in timelines, invisible in top-10 tables.
      "CREATE TABLE IF NOT EXISTS rollup_daily (
         date  DATE,
         dim   dim_t,
         value VARCHAR,
         cnt   BIGINT
       )")
    (.execute stmt
      "CREATE TABLE IF NOT EXISTS rollup_state (last_date DATE)")
    (.execute stmt
      "INSERT INTO rollup_state VALUES (DATE '1970-01-01')")))

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
           )")
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

(defn migrate-2->3! [^String db-path]
  (log "Migrating" db-path "to version 3")
  (with-open [conn (connect db-path)
              stmt (.createStatement conn)]
    (.execute stmt
      "CREATE TYPE IF NOT EXISTS dim_t AS ENUM ('feed', 'bot', 'browser', 'path', 'query', 'ref_domain')")
    (.execute stmt
      "CREATE TABLE IF NOT EXISTS daily_counts (
         date  DATE,
         dim   dim_t,
         value VARCHAR,
         cnt   BIGINT
       )")
    (.execute stmt
      "CREATE TABLE IF NOT EXISTS rollup_state (last_date DATE)")
    (.execute stmt
      "INSERT INTO rollup_state SELECT DATE '1970-01-01' WHERE NOT EXISTS (SELECT * FROM rollup_state)")
    (.execute stmt
      "UPDATE version SET version = 3"))
  (log "Migration to version 3 complete"))

(defn migrate-3->4! [^String db-path]
  (log "Migrating" db-path "to version 4")
  (with-open [conn (connect db-path)
              stmt (.createStatement conn)]
    (.execute stmt "BEGIN TRANSACTION")
    (.execute stmt
      (str
        "CREATE TABLE rollup_daily AS
           WITH ranked AS (
             SELECT date, dim, value, cnt,
                    ROW_NUMBER() OVER (PARTITION BY date, dim ORDER BY cnt DESC, value) AS rn
             FROM daily_counts
             WHERE value IS NOT NULL
           )
           SELECT date, dim, value, cnt FROM ranked WHERE rn <= " rollup-depth "
           UNION ALL
           SELECT date, dim, '" rollup-sentinel "', SUM(cnt)::BIGINT
           FROM ranked WHERE rn > " rollup-depth " GROUP BY date, dim
           UNION ALL
           SELECT date, dim, value, cnt FROM daily_counts WHERE value IS NULL"))
    (.execute stmt "DROP TABLE daily_counts")
    (.execute stmt "UPDATE version SET version = 4")
    (.execute stmt "COMMIT"))
  (log "Migration to version 4 complete"))

(defn- rollup-day!
  "Aggregates one day of stats into rollup_daily and advances rollup_state to it"
  [^DuckDBConnection conn ^LocalDate date]
  (log-verbose "Rolling up" (str date))
  (.setAutoCommit conn false)
  (try
    (with-open [stmt (.prepareStatement conn "DELETE FROM rollup_daily WHERE date = ?")]
      (.setObject stmt 1 date)
      (.execute stmt))
    (with-open [stmt (.prepareStatement conn
                       (str
                         "INSERT INTO rollup_daily
                            WITH agg AS (
                              SELECT date, type::VARCHAR::dim_t AS dim, agent AS value, SUM(mult) AS cnt
                              FROM (SELECT date, type, ANY_VALUE(agent) AS agent, uniq, MAX(mult) AS mult
                                    FROM stats
                                    WHERE date = ?
                                    GROUP BY date, type, uniq)
                              GROUP BY date, type, agent
                            ),
                            ranked AS (
                              SELECT *, ROW_NUMBER() OVER (PARTITION BY dim ORDER BY cnt DESC, value) AS rn
                              FROM agg
                              WHERE value IS NOT NULL
                            )
                            SELECT date, dim, value, cnt FROM ranked WHERE rn <= " rollup-depth "
                            UNION ALL
                            SELECT date, dim, '" rollup-sentinel "', SUM(cnt)
                            FROM ranked WHERE rn > " rollup-depth " GROUP BY date, dim
                            UNION ALL
                            SELECT date, dim, value, cnt FROM agg WHERE value IS NULL"))]
      (.setObject stmt 1 date)
      (.execute stmt))
    (doseq [dim ["path" "query" "ref_domain"]]
      (with-open [stmt (.prepareStatement conn
                         (str
                           "INSERT INTO rollup_daily
                              WITH agg AS (
                                SELECT date, " dim " AS value, COUNT(*) AS cnt
                                FROM stats
                                WHERE date = ? AND type = 'browser' AND " dim " IS NOT NULL
                                GROUP BY date, " dim "
                              ),
                              ranked AS (
                                SELECT *, ROW_NUMBER() OVER (ORDER BY cnt DESC, value) AS rn
                                FROM agg
                              )
                              SELECT date, '" dim "', value, cnt FROM ranked WHERE rn <= " rollup-depth "
                              UNION ALL
                              SELECT date, '" dim "', '" rollup-sentinel "', SUM(cnt)
                              FROM ranked WHERE rn > " rollup-depth " GROUP BY date"))]
        (.setObject stmt 1 date)
        (.execute stmt)))
    (with-open [stmt (.prepareStatement conn "UPDATE rollup_state SET last_date = ?")]
      (.setObject stmt 1 date)
      (.execute stmt))
    (.commit conn)
    (catch Throwable t
      (.rollback conn)
      (throw t))
    (finally
      (.setAutoCommit conn true))))

(def ^:private *rollup-dates
  "db-path -> in-memory mirror of rollup_state.last_date, so worker ticks
   with a current watermark don't open the database at all. Read from the
   database once after startup, then maintained in memory"
  (atom {}))

(defn rollup!
  "Rolls up every completed UTC day after rollup_state.last_date into rollup_daily,
   day by day, then advances the watermark to yesterday"
  [db-path]
  (let [yesterday (.minusDays (LocalDate/now UTC) 1)
        last-date (get @*rollup-dates db-path)]
    (when (or (nil? last-date) (LocalDate/.isBefore last-date yesterday))
      (with-open [conn (acquire-conn db-path)]
        (let [last-date (or last-date
                          (with-open [stmt (.createStatement conn)
                                      rs   (.executeQuery stmt "SELECT last_date FROM rollup_state")]
                            (when (.next rs)
                              ^LocalDate (.getObject rs 1))))]
          (when last-date
            (when (LocalDate/.isBefore last-date yesterday)
              (let [dates (with-open [stmt (doto (.prepareStatement conn
                                                   "SELECT DISTINCT date FROM stats WHERE date > ? AND date <= ? ORDER BY date")
                                             (.setObject 1 last-date)
                                             (.setObject 2 yesterday))
                                      rs   (.executeQuery stmt)]
                            (loop [acc []]
                              (if (.next rs)
                                (recur (conj acc (.getObject rs 1)))
                                acc)))]
                (doseq [date dates]
                  (rollup-day! conn date))
                ;; advance watermark over trailing empty days too
                (with-open [stmt (.prepareStatement conn "UPDATE rollup_state SET last_date = ?")]
                  (.setObject stmt 1 yesterday)
                  (.execute stmt))
                (log-verbose "Rolled up" (count dates) "day(s), watermark at" (str yesterday))))
            (swap! *rollup-dates assoc db-path yesterday)))))))

(defn rebuild-rollup!
  "Recompute rollup_daily from scratch"
  [db-path]
  (with-conn [conn db-path]
    (with-open [stmt (.createStatement conn)]
      (.execute stmt "DELETE FROM rollup_daily")
      (.execute stmt "UPDATE rollup_state SET last_date = DATE '1970-01-01'")))
  (swap! *rollup-dates dissoc db-path)
  (rollup! db-path))

(defn check-db [db-path]
  (if-not (File/.exists (io/file db-path))
    (with-open [conn (connect db-path)]
      (init-db! conn))
    (let [v (with-open [conn (connect db-path)]
              (db-version conn))]
      (when (<= v 1)
        (migrate-1->2! db-path))
      (when (<= v 2)
        (migrate-2->3! db-path))
      (when (<= v 3)
        (migrate-3->4! db-path)))))

(def ^:private *worker-pool
  (atom nil))

(def ^:private *worker-task
  (atom nil))

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
  (with-open [conn (acquire-conn db-path)
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
    (.shutdown ^ScheduledThreadPoolExecutor pool))
  (close-cached-conn!))

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
                     (insert-lines! db-path buf))
                   ;; runs after insert so a day is only rolled up once all its
                   ;; queued lines are in stats; no-op while watermark is current
                   (rollup! db-path))
                 (catch Exception e
                   (log e))
                 (finally
                   (close-cached-conn-if-expired!))))]
    (reset! *worker-pool pool)
    ;; first tick right away: after an upgrade it backfills rollup_daily
    ;; in the background while the site is already serving
    (reset! *worker-task (.scheduleAtFixedRate pool ^Runnable task 0 1 TimeUnit/MINUTES))))

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
   (with-open [conn (acquire-conn db-path {:reset-ttl? true})]
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
