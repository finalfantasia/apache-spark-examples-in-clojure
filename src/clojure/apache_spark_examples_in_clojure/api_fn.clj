(ns apache-spark-examples-in-clojure.api-fn
  (:import
    (org.apache.spark.api.java.function MapFunction FlatMapFunction)))


(defn ->map-fn
  ^MapFunction
  [f]
  (reify MapFunction
    (call [_ x]
      (f x))))


(defn ->flat-map-fn
  ^FlatMapFunction
  [f]
  (reify FlatMapFunction
    (call [_ x]
      (.iterator ^Iterable (f x)))))
