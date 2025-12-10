(defproject io.github.tonsky/clj-simple-stats "0.0.0"
  :description "<description>"
  :license     {:name "MIT" :url "https://github.com/tonsky/clj-simple-stats/blob/master/LICENSE"}
  :url         "https://github.com/tonsky/clj-simple-stats"
  :dependencies
  [[org.clojure/clojure "1.12.3"]]
  :deploy-repositories
  {"clojars"
   {:url "https://clojars.org/repo"
    :username "tonsky"
    :password :env/clojars_token
    :sign-releases false}})