import Dependencies._

ThisBuild / scalaVersion     := "2.13.12"
ThisBuild / version          := "0.1.0"
ThisBuild / organization     := "com.bdad"
ThisBuild / organizationName := "bdad"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF","services",xs @ _*) => MergeStrategy.filterDistinctLines // Added this
  case PathList("META-INF",xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

lazy val bdad = (project in file("."))
  .settings(
      assembly / mainClass := Some("producer.KafkaProducer"),
      libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0",
      libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0" ,
      libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0"
  )


lazy val root = (project in file("."))
  .settings(
    name := "bdad",
    libraryDependencies += munit % Test,
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0" ,
    libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0"
)

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
