(ns clj-simple-stats.import
  (:require
   [clj-simple-stats.analyzer :as analyzer]
   [clj-simple-stats.core :as core]
   [clojure.java.io :as io]
   [clojure.string :as str])
  (:import
   [java.nio ByteBuffer]
   [java.security MessageDigest]
   [java.time LocalDate LocalDateTime LocalTime]
   [java.time.format DateTimeFormatter]
   [java.util UUID]
   [org.duckdb DuckDBConnection]))

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

(defn parse-instant [s]
  (LocalDateTime/parse s instant-formatter))

(defn parse-line-nginx [line]
  (let [[ip _ user time request status bytes referrer user-agent]
        (->> line
          (re-seq #"-|\"-\"|\"([^\"]+)\"|\[([^\]]+)\]|([^\"\[\] ]+)")
          (map next)
          (map (fn [[a b c]] (or a b c))))
        [method url protocol] (str/split request #"\s+")]
    (when url
      (let [[_ path query fragment] (re-matches #"([^?#]+)(?:\?([^#]+)?)?(?:#(.+)?)?" url)]
        {:ip         ip
         :user       user
         :time       (parse-instant time)
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
         :user-agent user-agent}))))

(comment
  (with-open [rdr (io/reader (io/file "/Users/tonsky/Downloads/tonsky_logs/access.log.1"))]
    (->> (line-seq rdr)
      (map parse-line-nginx)
      #_(filter #(= 200 (:status %)))
      (filter #(= "https://tonsky.me/" (:referrer %)))
      #_(map :url)
      #_(set)
      (take 100)
      (doall))))

#_(defn convert-nginx [from to]
  (with-open [rdr (io/reader (io/file from))
              wrt (io/writer (io/file to))]
    (.write wrt "date\ttime\tpath\tquery\tip\tuser-agent\treferrer\n")
    (doseq [:let [lines (keep parse-line-nginx (line-seq rdr))]
            {:keys [time path query fragment ip user-agent referrer status]} lines
            :when (= 200 status)]
      (.write wrt
        (str
          (.format date-formatter time) "\t"
          (.format time-formatter time) "\t"
          path       "\t"
          query      "\t"
          ip         "\t"
          user-agent "\t"
          referrer   "\n")))))

(comment
  (convert-nginx "grumpy_data/stats/grumpy_access.log" "grumpy_data/stats/2023-08.csv")
  (convert-nginx "grumpy_data/stats/grumpy_access.log.1" "grumpy_data/stats/2023-07.csv")
  (convert-nginx "grumpy_data/stats/grumpy_access.log.2" "grumpy_data/stats/2023-06.csv")
  (convert-nginx "grumpy_data/stats/grumpy_access.log.3" "grumpy_data/stats/2023-05.csv")
  (convert-nginx "grumpy_data/stats/grumpy_access.log.4" "grumpy_data/stats/2023-04.csv")
  (convert-nginx "grumpy_data/stats/grumpy_access.log.5" "grumpy_data/stats/2023-03.csv")
  (convert-nginx "grumpy_data/stats/grumpy_access.log.6" "grumpy_data/stats/2023-02.csv")
  (convert-nginx "grumpy_data/stats/grumpy_access.log.7" "grumpy_data/stats/2023-01.csv")
  (convert-nginx "grumpy_data/stats/grumpy_access.log.8" "grumpy_data/stats/2022-12.csv"))
