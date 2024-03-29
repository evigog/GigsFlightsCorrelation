name := "spark_project"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1",
  //"org.apache.spark" %% "spark-sql" % "2.2.1",
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.1",
  //"org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.2.1",
  //("com.datastax.spark" %% "spark-cassandra-connector" % "2.0.2").exclude("io.netty", "netty-handler"),
  //("com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0").exclude("io.netty", "netty-handler"),
)

libraryDependencies += "joda-time" % "joda-time" % "2.9.3"
libraryDependencies += "org.joda" % "joda-convert" % "1.8"

