(ns apache-spark-examples-in-clojure.spark-sql-example
  (:require
    [apache-spark-examples-in-clojure.col :as c]
    [apache-spark-examples-in-clojure.api-fn :as api]
    [apache-spark-examples-in-clojure.util :as u])
  (:import
    (org.apache.spark.sql SparkSession Encoders)
    (apache_spark_examples_in_clojure Person)))


(defn run-basic-data-frame-example!
  [^SparkSession spark]
  (let [df (-> spark
               (.read)
               (.json "resources/people.json"))]


    ;; Prints the content of the DataFrame to STDOUT.
    (.show df)
    ;; +----+-------+
    ;; | age|   name|
    ;; +----+-------+
    ;; |null|Michael|
    ;; |  30|   Andy|
    ;; |  19| Justin|
    ;; +----+-------+


    ;; Prints the schema in a tree format.
    (.printSchema df)
    ;; root
    ;; |-- age: long (nullable = true)
    ;; |-- name: string (nullable = true)))


    ;; Selects only the "name" column.
    (-> df
        (.select (c/cols "name"))
        (.show))
    ;; +-------+
    ;; |   name|
    ;; +-------+
    ;; |Michael|
    ;; |   Andy|
    ;; | Justin|
    ;; +-------+


    ;; Selects everybody but increments "age" by 1.
    (-> df
        (.select (c/cols (c/col "name") (.plus (c/col "age") 1)))
        (.show))
    ;; +-------+---------+
    ;; |   name|(age + 1)|
    ;; +-------+---------+
    ;; |Michael|     null|
    ;; |   Andy|       31|
    ;; | Justin|       20|
    ;; +-------+---------+


    ;; Selects people older than 21.
    (-> df
        (.filter (.gt (c/col "age") 21))
        (.show))
    ;; +---+----+
    ;; |age|name|
    ;; +---+----+
    ;; | 30|Andy|
    ;; +---+----+


    ;; Counts people by age.
    (-> df
        (.groupBy (c/cols "age"))
        (.count)
        (.show))
    ;; +----+-----+
    ;; | age|count|
    ;; +----+-----+
    ;; |  19|    1|
    ;; |null|    1|
    ;; |  30|    1|
    ;; +----+-----+


    ;; Registers the DataFrame as a SQL temporary view.
    (.createOrReplaceTempView df "people")

    (let [sql-df (.sql spark "SELECT * FROM people")]
      (.show sql-df))
    ;; +----+-------+
    ;; | age|   name|
    ;; +----+-------+
    ;; |null|Michael|
    ;; |  30|   Andy|
    ;; |  19| Justin|
    ;; +----+-------+


    ;; Registers the DataFrame as a global temporary view.
    (.createOrReplaceGlobalTempView df "people")

    ;; Global temporary view is tied to a system-preserved database, 'global_temp'.
    (-> spark
        (.sql "SELECT * FROM global_temp.people")
        (.show))
    ;; +----+-------+
    ;; | age|   name|
    ;; +----+-------+
    ;; |null|Michael|
    ;; |  30|   Andy|
    ;; |  19| Justin|
    ;; +----+-------+


    ;; Global temporary view is cross-session.
    (-> spark
        (.newSession)
        (.sql "SELECT * FROM global_temp.people")
        (.show))))
    ;; +----+-------+
    ;; | age|   name|
    ;; +----+-------+
    ;; |null|Michael|
    ;; |  30|   Andy|
    ;; |  19| Justin|
    ;; +----+-------+


(defn run-dataset-creation-example!
  [^SparkSession spark]
  (let [;; Creates an instance of a Bean class.
        person         (doto (Person.)
                             (.setName "Andy")
                             (.setAge 32))

        ;; Encoders are created for Java beans.
        person-encoder (Encoders/bean Person)
        java-bean-ds   (.createDataset spark
                                       (u/array-list [person])
                                       person-encoder)]

    (.show java-bean-ds)
    ;; +---+----+
    ;; |age|name|
    ;; +---+----+
    ;; | 32|Andy|
    ;; +---+----+


    ;; Encoders for most common types are provided in class Encoders.
    (let [integer-encoder (Encoders/LONG)
          primitive-ds    (.createDataset spark
                                          (u/array-list [1, 2, 3])
                                          integer-encoder)
          transformed-ds  (.map primitive-ds
                                (api/map-fn inc)
                                integer-encoder)]
      (.collect transformed-ds)) ;; Returns [2, 3, 4]


    ;; DataFrames can be converted to a Dataset by providing a class, mapping based on name.
    (let [path      "resources/people.json"
          people-ds (-> spark
                        (.read)
                        (.json path)
                        (.as person-encoder))]
      (.show people-ds))))
      ;; +----+-------+
      ;; | age|   name|
      ;; +----+-------+
      ;; |null|Michael|
      ;; |  30|   Andy|
      ;; |  19| Justin|
      ;; +----+-------+


(defn -main
  [& _]
  (let [spark (-> (SparkSession/builder)
                  (.appName "Spark SQL basic example")
                  (.master "local")
                  (.config "apache_spark_examples_in_clojure.some.config.option" "some-value")
                  (.getOrCreate))]

    (run-basic-data-frame-example! spark)
    (run-dataset-creation-example! spark)

    (.stop spark)))
