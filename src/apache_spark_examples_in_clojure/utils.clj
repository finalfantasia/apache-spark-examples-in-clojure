(ns apache-spark-examples-in-clojure.utils
  (:import
    (java.util List Collection ArrayList)
    (org.apache.spark.sql Column functions)))


(defn ->array-list
  ^List
  [^Collection coll]
  (ArrayList. coll))


(defmulti col "Creates a `Column` from given argument." class)
(defmethod col Column ^Column [^Column x] x)
(defmethod col String ^Column [^String x] (functions/col x))


(defn cols
  "Creates an array of `Column`s."
  #^"[Lorg.apache.spark.sql.Column;"
  [& xs]
  (into-array Column (map col xs)))
