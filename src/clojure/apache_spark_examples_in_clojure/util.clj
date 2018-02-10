(ns apache-spark-examples-in-clojure.util
  (:import
    (java.util List Collection ArrayList)))


(defn ->array-list
  ^List
  [^Collection coll]
  (ArrayList. coll))
