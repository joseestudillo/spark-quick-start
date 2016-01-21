// JUnit support for java
libraryDependencies += "junit" % "junit" % "4.8.1" % "test"

// ScalaTestSDK
libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

// Logging
libraryDependencies += "com.typesafe.scala-logging" % "scala-logging_2.11" % "3.1.0"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.12"

// Akka concurrency as scala packages for concurrency are marked as deprecated
libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.4.1"

// Spark dependencies
libraryDependencies += "log4j" % "log4j" % "1.2.16"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-repl_2.11" % "1.6.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0"
libraryDependencies += "org.apache.derby" % "derby" % "10.11.1.1"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.4"

// Allow to see scala sources in eclipse, It requires the eclipse plugin installed
EclipseKeys.withSource := true

lazy val commonSettings = Seq(
  name := "spark",
  organization := "com.joseestudillo",
  version := "0.1.0",
  scalaVersion := "2.11.7"
)

