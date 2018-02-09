(ns apache-spark-examples-in-clojure.spark-sql-example
  (:require
    [apache-spark-examples-in-clojure.utils :as u])
  (:import
    (org.apache.spark.sql SparkSession functions)))


(defn run-basic-data-frame-example! [^SparkSession spark]
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
        (.select (u/cols "name"))
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
        (.select (u/cols (functions/col "name") (.plus (functions/col "age") 1)))
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
        (.filter (.gt (functions/col "age") 21))
        (.show))
    ;; +---+----+
    ;; |age|name|
    ;; +---+----+
    ;; | 30|Andy|
    ;; +---+----+


    ;; Counts people by age.
    (-> df
        (.groupBy (u/cols "age"))
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

    ;; Global temporary view is tied to a system preserved database 'global_temp'.
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


(defn -main
  [& args]
  (let [spark (-> (SparkSession/builder)
                  (.appName "Spark SQL basic example")
                  (.master "local")
                  (.config "spark.some.config.option" "some-value")
                  (.getOrCreate))]

    (run-basic-data-frame-example! spark)

    (.stop spark)))
