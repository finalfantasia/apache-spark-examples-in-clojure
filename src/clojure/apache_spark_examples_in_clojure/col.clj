(ns apache-spark-examples-in-clojure.col
  (:import
    (org.apache.spark.sql functions Column)))

(defn col
  "Creates a `Column` from column name."
  ^Column
  [^String x]
  (functions/col x))


(defmulti col-helper class)
(defmethod col-helper Column [x] x)
(defmethod col-helper String [x] (col x))

(defn cols
  "Creates an array of `Column`s from column names or columns."
  #^"[Lorg.apache.spark.sql.Column;"
  [& xs]
  (into-array Column (map col-helper xs)))
