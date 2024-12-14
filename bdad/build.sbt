import Dependencies._

// ThisBuild settings
ThisBuild / scalaVersion     := "2.13.12"
ThisBuild / version          := "0.1.0"
ThisBuild / organization     := "com.bdad"
ThisBuild / organizationName := "bdad"

// Assembly merge strategy
ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF","services",xs @ _*) => MergeStrategy.filterDistinctLines // Added this
  case PathList("META-INF",xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

// Producer project
lazy val producer = (project in file("producer"))
  .settings(
    name := "producer",
    assembly / mainClass := Some("producer.KafkaProducer"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.apache.spark" %% "spark-sql" % "3.5.0",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",
    )
  )

// Consumer project
lazy val consumer = (project in file("consumer"))
  .settings(
    name := "consumer",
    assembly / mainClass := Some("consumer.KafkaConsumer"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.apache.spark" %% "spark-sql" % "3.5.0",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",
      "org.apache.hudi" %% "hudi-spark3.5-bundle" % "0.15.0",
    )
  )
