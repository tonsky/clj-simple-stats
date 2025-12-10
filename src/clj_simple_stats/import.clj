(ns clj-simple-stats.import
  (:require
   [clj-simple-stats.analyzer :as analyzer]
   [clj-simple-stats.core :as core]
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.string :as str])
  (:import
   [java.io File]
   [java.nio ByteBuffer]
   [java.security MessageDigest]
   [java.time LocalDate LocalDateTime LocalTime]
   [java.time.format DateTimeFormatter]
   [java.util UUID]
   [org.duckdb DuckDBAppender DuckDBConnection]))

(defn update-to-new [db-in db-out]
  (core/with-conn [conn-in db-in]
    (core/with-conn [conn-out db-out]
      (core/init-db! conn-out)
      (with-open [stmt (.createStatement conn-in)
                  apnd (.createAppender conn-out DuckDBConnection/DEFAULT_SCHEMA "stats")]
        (let [rs (.executeQuery stmt "SELECT * FROM stats")
              t0 (System/currentTimeMillis)]
          (loop [acc 0]
            (when (= 0 (mod acc 10000))
              (.flush apnd)
              (println (- (System/currentTimeMillis) t0) "ms" acc "rows"))

            (if (.next rs)
              (let [line {:date       (.getObject rs  1)
                          :time       (.getObject rs  2)
                          :path       (.getString rs  3)
                          :query      (.getString rs  4)
                          :ip         (.getString rs  5)
                          :user-agent (.getString rs  6)
                          :referrer   (.getString rs  7)
                          :type       (.getString rs  8)
                          :agent      (.getString rs  9)
                          :os         (.getString rs 10)
                          :ref-domain (.getString rs 11)
                          :mult       (.getInt    rs 12)
                          :set-cookie (.getObject rs 13)
                          :uniq       (.getObject rs 14)}
                    line' (assoc line :ref-domain (analyzer/line-ref-domain line))]
                (.beginRow apnd)
                (.append apnd ^LocalDate (:date line'))
                (.append apnd ^LocalTime (:time line'))
                (.append apnd ^String    (:path line'))
                (.append apnd ^String    (:query line'))
                (.append apnd ^String    (:ip line'))
                (.append apnd ^String    (:user-agent line'))
                (.append apnd ^String    (:referrer line'))
                (.append apnd ^String    (:type line'))
                (.append apnd ^String    (:agent line'))
                (.append apnd ^String    (:os line'))
                (.append apnd ^String    (:ref-domain line'))
                (.append apnd            (int (:mult line')))
                (.append apnd ^UUID      (:set-cookie line'))
                (.append apnd ^UUID      (:uniq line'))
                (.endRow apnd)
                (recur (inc acc)))

              (do
                (.flush apnd)
                (println (- (System/currentTimeMillis) t0) "ms" acc "rows")
                :done))))))))

(comment
  (update-to-csv
    "grumpy_data/stats_2025_12-09.duckdb"
    "grumpy_data/stats_2025_12-09_domain.duckdb"))

(def ^DateTimeFormatter instant-formatter
  (DateTimeFormatter/ofPattern "dd/MMM/yyyy:HH:mm:ss X"))

(defn parse-instant ^LocalDateTime [s]
  (LocalDateTime/parse s instant-formatter))

(defn parse-line-nginx [line]
  (try
    (let [[ip _ user time request status bytes referrer user-agent]
          (->> line
            (re-seq #"-|\"-\"|\"([^\"]+)\"|\[([^\]]+)\]|([^\"\[\] ]+)")
            (map next)
            (map (fn [[a b c]] (or a b c))))
          [method url protocol] (some-> request (str/split #"\s+"))]
      (when (not (some str/blank? [method url protocol]))
        (let [[_ path query fragment] (re-matches #"([^?#]+)(?:\?([^#]+)?)?(?:#(.+)?)?" url)
              inst (parse-instant time)]
          {:ip         ip
           :user       user
           :date       (.toLocalDate inst)
           :time       (.toLocalTime inst)
           :request    request
           :method     method
           :url        url
           :path       path
           :query      query
           :fragment   fragment
           :protocol   protocol
           :status     (some-> status parse-long)
           :bytes      (some-> bytes parse-long)
           :referrer   referrer
           :user-agent user-agent
           :type       (when (str/includes? path "atom.xml")
                         "feed")})))
    (catch Exception e
      (println (ex-message e) (pr-str line))
      nil)))

(comment
  (parse-line-nginx "39.62.10.126 - - [17/Sep/2023:04:46:33 +0000] \"-\" 400 166 \"-\" \"-\""))

(defn valid-line? [{:keys [status time path ip user-agent]}]
  (and time
    (= 200 status)
    (not (some str/blank? [path ip user-agent]))
    (not (re-find #"(?i)/static|\.svg|\.png|\.ico|\.jpe?g|\.mp4|\.gif|\.php|\.txt|\.webp|\.woff|\.md|\.DS_Store|\.css|\.js" path))
    (not (#{"/forbidden" "/robots.txt"} path))))

(def t0
  (System/currentTimeMillis))

(defn dt []
  (- (System/currentTimeMillis) t0))

(defn nginx->csv [^File input]
  (let [name   (.getName input)
        output (io/file (str (File/.getPath input) ".csv"))]
    (if (.exists output)
      (println "Skipping" name)
      (with-open [rdr (io/reader input)]
        (let [lines (->> (line-seq rdr)
                      (keep parse-line-nginx)
                      (filter valid-line?)
                      (map analyzer/analyze)
                      (map (juxt :date :time :path :query :ip :user-agent :referrer :type :agent :os :ref-domain :mult :set-cookie :uniq)))]
          (with-open [writer (io/writer output)]
            (csv/write-csv writer
              (cons ["date" "time" "path" "query" "ip" "user_agent" "referrer" "type" "agent" "os" "ref_domain" "mult" "set_cookie" "uniq"]
                lines))))
        (println (format "%,d ms %s" (dt) name))))))

(comment
  (doseq [:let [_ (alter-var-root #'t0 (constantly (System/currentTimeMillis)))]
          file (->> (file-seq (io/file "/Users/tonsky/Downloads/tonsky_logs/"))
                 (filter #(re-find #"site_access" (File/.getPath %)))
                 (remove #(re-find #"\.csv" (File/.getName %)))
                 (sort-by File/.getPath))]
    (nginx->csv file))

  (.delete (io/file "/Users/tonsky/Downloads/tonsky_logs/stats.duckdb"))
  (core/with-conn [conn "/Users/tonsky/Downloads/tonsky_logs/stats.duckdb"] {:ttl 0}
    (core/init-db! conn)))