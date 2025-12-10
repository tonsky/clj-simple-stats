(ns user
  (:require
   [clj-reload.core :as clj-reload]
   [clojure+.hashp :as hashp]
   [clojure+.print :as print]
   [clojure+.error :as error]
   [clojure+.test :as test]))

(hashp/install!)
(print/install!)
(error/install!)


; reload

(clj-reload/init
  {:dirs      ["src" "dev" "test"]
   :no-reload '#{user}})

(def reload
  clj-reload/reload)


; tests

(test/install!)

(def test-re
  #"clj-simple-stats\..*-test")

(defn test-all []
  (clj-reload/reload {:only test-re})
  (test/run test-re))

(defn test-exit [_opts]
  (let [{:keys [fail error]} (test-all)]
    (System/exit (+ fail error))))
