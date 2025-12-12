(defproject io.github.tonsky/clj-simple-stats "0.0.0"
  :description "Simple statistics for Clojure/Ring web apps"
  :license     {:name "MIT" :url "https://github.com/tonsky/clj-simple-stats/blob/master/LICENSE"}
  :url         "https://github.com/tonsky/clj-simple-stats"
  :dependencies
  [[org.clojure/clojure    "1.12.4"]
   [org.duckdb/duckdb_jdbc "1.4.3.0"]
   [ring/ring-core         "1.15.3"]]
  :deploy-repositories
  {"clojars"
   {:url           "https://clojars.org/repo"
    :username      "tonsky"
    :password      :env/clojars_token
    :sign-releases false}})
