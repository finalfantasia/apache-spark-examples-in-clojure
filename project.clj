(defproject apache-spark-examples-in-clojure "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.apache.spark/spark-core_2.11 "2.2.1"]
                 [org.apache.spark/spark-sql_2.11 "2.2.1"]]

  :main ^:skip-aot apache-spark-examples-in-clojure.core

  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :target-path "target/%s"

  :profiles {:uberjar {:aot :all}})
