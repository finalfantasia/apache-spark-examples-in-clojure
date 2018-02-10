(ns apache-spark-examples-in-clojure.utils
  (:import
    (java.util List Collection ArrayList)
    (org.apache.spark.sql Column functions)))


(defn ->array-list
  ^List
  [^Collection coll]
  (ArrayList. coll))


(defn col
  "Creates a `Column` from column name."
  ^Column
  [^String x]
  (functions/col x))


(defmulti col-helper class)
(defmethod col-helper Column [x] x)
(defmethod col-helper String [x] (functions/col x))


(defn cols
  "Creates an array of `Column`s."
  #^"[Lorg.apache.spark.sql.Column;"
  [& xs]
  (into-array Column (map col-helper xs)))
