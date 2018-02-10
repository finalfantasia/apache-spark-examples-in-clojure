(ns apache-spark-examples-in-clojure.core
  (:gen-class)
  (:require
    [apache-spark-examples-in-clojure.spark-sql-example :as sql-example]))


(defn -main
  [& _]
  (println "See the individual example namespaces.")
  (sql-example/-main))
