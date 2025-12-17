(ns clj-simple-stats.dashboard
  (:require
   [clojure.java.io :as io]
   [clojure.math :as math]
   [clojure.string :as str]
   [ring.middleware.params :as params])
  (:import
   [java.net URLEncoder]
   [java.sql DriverManager ResultSet]
   [java.time LocalDate]
   [java.time.format DateTimeFormatter]
   [java.time.temporal TemporalAdjusters]
   [org.duckdb DuckDBConnection]))

(def ^:private ^DateTimeFormatter date-time-formatter
  (DateTimeFormatter/ofPattern "MMM d"))

(def ^:private ^DateTimeFormatter year-month-formatter
  (DateTimeFormatter/ofPattern "yyyy-MM"))

(defn query [q reduce-fn init ^DuckDBConnection conn]
  (with-open [stmt (.createStatement conn)
              rs   (.executeQuery stmt q)]
    (loop [acc init]
      (if (.next rs)
        (recur (reduce-fn acc rs))
        acc))))

(defn visits-by-type+date [conn where]
  (->
    (query
      (str
        "WITH subq AS (
           SELECT type, date, MAX(mult) AS mult
           FROM stats
           WHERE " where "
           GROUP BY type, date, uniq
         )
         FROM subq
         SELECT type, date, SUM(mult) AS cnt
         GROUP BY type, date")
      (fn [acc ^ResultSet rs]
        (let [type (.getString rs 1)
              date (.getObject rs 2)
              cnt  (.getLong rs 3)]
          (update acc (keyword type) (fnil assoc! (transient {})) date cnt)))
      {} conn)
    (update-vals persistent!)))

(comment
  (clj-simple-stats.core/with-conn [conn "grumpy_data/stats.duckdb"]
    (visits-by-type+date conn "date <= '2025-12-31' AND date >= '2025-01-01'")))

(defn total-uniq [conn where]
  (query
    (str
      "WITH subq AS (
        SELECT type, MAX(mult) AS mult
        FROM stats
        WHERE " where "
        GROUP BY type, uniq
      )
      FROM subq
      SELECT type, SUM(mult) AS cnt
      GROUP BY type")
    (fn [acc ^ResultSet rs]
      (let [type (.getString rs 1)
            cnt  (.getLong rs 2)]
        (assoc acc (keyword type) cnt)))
    {} conn))

(comment
  (clj-simple-stats.core/with-conn [conn "grumpy_data/stats.duckdb"]
    (total-uniq conn "date <= '2025-12-31' AND date >= '2025-01-01'")))

(defn top-10 [conn what where]
  (->
    (query
      (str
        "WITH base_query AS (
           SELECT " what "
           FROM stats
           WHERE " where "
         ),
         top_values AS (
           FROM base_query
           SELECT
             " what " AS value,
             COUNT(*) AS count
           WHERE " what " IS NOT NULL
           GROUP BY value
           ORDER BY count DESC
         ),
         top_n AS (
           SELECT *
           FROM top_values
           ORDER BY count DESC
           LIMIT 10
         ),
         others AS (
           SELECT
             NULL AS value,
             COUNT(*) AS count
           FROM base_query
           WHERE " what " IS NOT NULL
           AND " what " NOT IN (SELECT value FROM top_n)
         )
         FROM top_n
         UNION ALL
         FROM others
         WHERE count > 0")
      (fn [acc ^ResultSet rs]
        (conj! acc [(.getString rs 1) (.getLong rs 2)]))
      (transient []) conn)
    persistent!))

(defn top-10-uniq [conn what where]
  (->
    (query
      (str
        "WITH base_query AS (
           SELECT ANY_VALUE(" what ") AS " what ", MAX(mult) AS mult
           FROM stats
           WHERE " where "
           GROUP BY uniq
         ),
         top_values AS (
           FROM base_query
           SELECT
             " what " AS value,
             SUM(mult) AS count
           WHERE " what " IS NOT NULL
           GROUP BY value
           ORDER BY count DESC
         ),
         top_n AS (
           SELECT *
           FROM top_values
           ORDER BY count DESC
           LIMIT 10
         ),
         others AS (
           SELECT
             NULL AS value,
             SUM(mult) AS count
           FROM base_query
           WHERE " what " IS NOT NULL
           AND " what " NOT IN (SELECT value FROM top_n)
         )
         FROM top_n
         UNION ALL
         FROM others
         WHERE count > 0")
      (fn [acc ^ResultSet rs]
        (conj! acc [(.getString rs 1) (.getLong rs 2)]))
      (transient []) conn)
    persistent!))

(comment
  (clj-simple-stats.core/with-conn [conn "grumpy_data/stats.duckdb"]
    #_(top-10 conn "path" "type = 'browser'")
    (top-10 conn "query" "type = 'browser'")
    #_(top-10 conn "query" "path = '/search' AND type = 'browser'")))

(defn styles []
  (slurp (io/resource "clj_simple_stats/style.css")))

(defn script []
  (slurp (io/resource "clj_simple_stats/script.js")))

(defn format-num [n]
  (->
    (cond
      (>= n 10000000) (format "%1.0fM" (/ n 1000000.0))
      (>= n  1000000) (format "%1.1fM" (/ n 1000000.0))
      (>= n    10000) (format "%1.0fK" (/ n    1000.0))
      (>= n     1000) (format "%1.1fK" (/ n    1000.0))
      :else           (str n))
    (str/replace ".0" "")))

(defn round-to [n m]
  (-> n
    (- 1)
    (/ m)
    Math/floor
    (+ 1)
    (* m)
    int))

(defn average [xs]
  (-> (reduce + 0 xs)
    (/ (max 1 (count xs)))
    math/round))

(def bar-w
  3)

(def graph-h
  100)

(defn encode-uri-component [s]
  (-> (URLEncoder/encode (str s) "UTF-8")
    (str/replace #"\+"   "%20")
    (str/replace #"\%21" "!")
    (str/replace #"\%27" "'")
    (str/replace #"\%28" "(")
    (str/replace #"\%29" ")")
    (str/replace #"\%7E" "~")))

(defn querystring [params]
  (str/join "&"
    (map
      (fn [[k v]]
        (str (name k) "=" (encode-uri-component v)))
      params)))

(defn page [conn req]
  (let [params (-> req params/params-request :query-params)
        {:strs [from to]} params
        today (LocalDate/now)]
    (if (or (nil? from) (nil? to))
      (let [from (.with (LocalDate/now) (TemporalAdjusters/firstDayOfYear))
            to   (.with (LocalDate/now) (TemporalAdjusters/lastDayOfYear))]
        {:status  302
         :headers {"Location" (str "?" (querystring (assoc params "from" from "to" to)))}})
      (let [from-date (LocalDate/parse from)
            to-date   (LocalDate/parse to)
            where     (str/join " AND "
                        (concat
                          [(str "date >= '" from "'")
                           (str "date <= '" to "'")]
                          (for [[k v] (dissoc params "from" "to")]
                            (str k " = '" (str/replace (str v) "'" "\\'") "'"))))
            sb        (StringBuilder.)
            append    #(do
                         (doseq [s %&]
                           (.append sb (str s)))
                         (.append sb "\n"))]
        (append "<!DOCTYPE html>")
        (append "<html>")
        (append "<head>")
        (append "<meta charset=\"utf-8\">")
        (append "<link rel='icon' href='" (:uri req) "/favicon.ico' sizes='32x32'>")
        (append "<link rel=\"preconnect\" href=\"https://fonts.gstatic.com\" crossorigin>")
        (append "<link href=\"https://fonts.googleapis.com/css2?family=Inter:opsz,wght@14..32,100..900&display=swap\" rel=\"stylesheet\">")
        (append "<style>" (styles) "</style>")
        (append "<script>" (script) "</script>")
        (append "</head>")
        (append "<body>")

        ;; filters
        (let [{:keys [^LocalDate min-date
                      ^LocalDate max-date]} (query "SELECT min(date), max(date) FROM stats"
                                              (fn [acc ^ResultSet rs]
                                                (assoc acc
                                                  :min-date (.getObject rs 1)
                                                  :max-date (.getObject rs 2)))
                                              {} conn)
              min-date       (or min-date (.with (LocalDate/now) (TemporalAdjusters/firstDayOfYear)))
              max-date       (or max-date (.with (LocalDate/now) (TemporalAdjusters/lastDayOfYear)))
              min-year       (.getYear min-date)
              max-year       (.getYear max-date)]
          (append "<div class=filters>")

          ;; years
          (append "<a class=filter href='?" (querystring (assoc params
                                                           "from" min-date
                                                           "to" max-date)) "'>All</a>")
          (doseq [^long year (range min-year (inc max-year))
                  :let [qs (querystring (assoc params
                                          "from" (str year "-01-01")
                                          "to"   (str year "-12-31")))]]
            (append "<a href='?" qs "' class='filter")
            (when-not (or
                        (< (.getYear to-date) year)
                        (> (.getYear from-date) year))
              (append " in"))
            (append "'>" year "</a>"))

          ;; other params
          (doseq [[k v] (dissoc params "from" "to")]
            (append "<div class=filter>" k ": " v)
            (append "<a href='?" (querystring (dissoc params k)) "'>×</a>")
            (append "</div>")) ;; .filter

          (append "</div>")) ;; .filters

        ;; timelines
        (let [data     (visits-by-type+date conn where)
              totals   (total-uniq conn where)
              max-val  (->> data
                         (mapcat (fn [[_type date->cnt]] (vals date->cnt)))
                         (reduce max 1))
              max-val  (cond
                         (>= max-val 200000) (round-to max-val 100000)
                         (>= max-val  20000) (round-to max-val  10000)
                         (>= max-val   2000) (round-to max-val   1000)
                         (>= max-val    100) (round-to max-val    100)
                         :else                                    100)
              dates    (stream-seq! (LocalDate/.datesUntil from-date (.plusDays to-date 1)))
              graph-w  (* (count dates) bar-w)

              bar-h    #(-> % (* graph-h) (/ max-val) int)
              hrz-step (cond
                         (>= max-val 600000) 200000
                         (>= max-val 300000) 100000
                         (>= max-val 100000)  50000

                         (>= max-val  60000)  20000
                         (>= max-val  30000)  10000
                         (>= max-val  10000)   5000

                         (>= max-val   6000)   2000
                         (>= max-val   3000)   1000
                         (>= max-val   1000)    500

                         (>= max-val    600)    200
                         (>= max-val    300)    100
                         (>= max-val    100)     50

                         (>= max-val     60)     20
                         :else                   10)]

          (doseq [[type title] [[:browser "Unique visitors"]
                                [:feed "RSS Readers"]
                                [:bot "Scrapers"]]
                  :let [date->cnt (get data type)]
                  :when (not (empty? date->cnt))]
            (case type
              :feed
              (append (format "<h1>%s: ~%,d / day</h1>" title (average (vals date->cnt))))
              #_else
              (append (format "<h1>%s: %,d</h1>" title (get totals type))))
            (append "<div class=graph_outer>")

            ;; .graph
            (append "<div class=graph_scroll>")
            (append "<svg class=graph width=" graph-w " height=" (+ graph-h 30) ">")

            ;; horizontal lines
            (doseq [val (range 0 (inc max-val) hrz-step)
                    :let [bar-h (bar-h val)]]
              (append "<line class=hrz x1=0 y1=" (- graph-h bar-h -10) " x2=" graph-w " y2=" (- graph-h bar-h -10) " />"))

            (doseq [[idx ^LocalDate date] (map vector (range) dates)
                    :let [val (get date->cnt date)]]
              ;; graph bar
              (when val
                (let [bar-h  (bar-h val)
                      data-v (format "%,d" val)
                      data-d (.format date-time-formatter date)
                      x      (* idx bar-w)
                      y      (- graph-h bar-h -10)]
                  (append "<g data-v='" data-v "' data-d='" data-d "'>")
                  (append "<rect class=i x=" x " y=8 width=" bar-w " height=" (+ graph-h 2) " />")
                  (append "<rect x=" x " y=" (- y 2) " width=" bar-w " height=" (+ bar-h 2) " />")
                  (append "<line x1=" x " y1=" (- y 1) " x2=" (+ x bar-w) " y2=" (- y 1) " />")
                  (append "</g>")))
              ;; month label
              (when (= 1 (.getDayOfMonth date))
                (let [month-end (.with date (TemporalAdjusters/lastDayOfMonth))
                      qs        (querystring (assoc params "from" date "to" month-end))
                      x         (* idx bar-w)]
                  (append "<line class=date x1=" x " y1=" (+ 12 graph-h) " x2=" x " y2=" (+ 20 graph-h) " />")
                  (append "<a href='?" qs "'>")
                  (append "<text x=" x " y=" (+ 30 graph-h) ">" (.format year-month-formatter date) "</text>")
                  (append "</a>")))

              ;; today
              (when (= today date)
                (append "<line class=today x1=" (* (+ idx 0.5) bar-w) " y1=0 x2=" (* (+ idx 0.5) bar-w) " y2=" (+ 20 graph-h) " />")))

            (append "</svg>") ;; .graph
            (append "</div>") ;; .graph_scroll

            ;;.graph_legend
            (append "<svg class=graph_legend height=" (+ graph-h 30) ">")
            (doseq [val (range 0 (inc max-val) hrz-step)
                    :let [bar-h (bar-h val)]]
              (append "<text x=20 y=" (- graph-h bar-h -13) " text-anchor=end>" (format-num val) "</text>"))
            (append "</svg>") ;; .graph_legend

            (append "<div class=graph_hover style='display: none'></div>")

            (append "</div>"))) ;; .graph_outer

        ;; top Ns
        (let [tbl (fn [title data & [opts]]
                    (when-not (empty? data)
                      (append "<div class=table_outer>")
                      (append "<h1>" title "</h1>")
                      (append "<table>")
                      (doseq [:let [{:keys [param href-fn]} opts
                                    total (max 1 (transduce (map second) + 0 data))]
                              [value count] data
                              :let [percent     (* 100.0 (/ count total))
                                    percent-str (if (< percent 2.0)
                                                  (format "%.1f%%" percent)
                                                  (format "%.0f%%" percent))]
                              :when (pos? count)]
                        (append "<tr>")
                        (append "<td class=f>")
                        (when (and param value)
                          (append "<a href='?" (querystring (assoc params param value)) "' title='Filter by " param " = " value "'>🔍</a>"))
                        (append "</td>")
                        (append "<th>")
                        (append "<div style='width: " percent-str "'" (when (nil? value) " class=other") "></div>")
                        (if (and href-fn value)
                          (append "<a href='" (href-fn value) "' title='" value "' target=_blank>" value "</a>")
                          (append "<span title='" (or value "Others") "'>" (or value "Others") "</span>"))
                        (append "</th>")
                        (append "<td>" (format-num count) "</td>")
                        (append "<td class='pct'>" percent-str "</td>")
                        (append "</tr>"))
                      (append "</table>")
                      (append "</div>")))]
          (append "<div class=tables>")
          (tbl "Paths"       (top-10      conn "path"       (str "type = 'browser' AND " where)) {:param "path", :href-fn identity})
          (tbl "Queries"     (top-10      conn "query"      (str "type = 'browser' AND " where)) {:param "query"})
          (tbl "Referrers"   (top-10      conn "ref_domain" (str "type = 'browser' AND " where)) {:param "ref_domain", :href-fn #(str "https://" %)})
          (tbl "Browsers"    (top-10-uniq conn "agent"      (str "type = 'browser' AND " where)) {:param "agent"})
          #_(tbl "OSes"        (top-10-uniq conn "os"         (str "type = 'browser' AND " where)) {:param "os"})
          (tbl "RSS Readers" (top-10-uniq conn "agent"      (str "type = 'feed'    AND " where)) {:param "agent"})
          (tbl "Scrapers"    (top-10-uniq conn "agent"      (str "type = 'bot'     AND " where)) {:param "agent"})
          (append "</div>"))

        (append "</body>")
        (append "</html>")
        {:status  200
         :headers {"Content-Type" "text/html; charset=utf-8"}
         :body    (.toString sb)}))))
