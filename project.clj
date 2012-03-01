(defproject cascading-mongomigrate "0.0.1"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"conjars" "http://conjars.org/repo/"}
  :dependencies [[cascading/cascading-core "1.2.4"
                  :exclusions [org.codehaus.janino/janino
                               thirdparty/jgrapht-jdk1.6
                               riffle/riffle]]
                 [org.mongodb/mongo-java-driver "2.7.2"]
                 [thirdparty/jgrapht-jdk1.6 "0.8.1"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [lein-clojars "0.7.0"]])
