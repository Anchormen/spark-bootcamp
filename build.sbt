name := "Anchormen SparkTraining"

/** Common settings for all projects **/
lazy val settings = Seq(
  organization := "nl.anchormen",
  isSnapshot := true,
  version := "0.0.1-SNAPSHOT",
  crossPaths := false,
  scalaVersion := "2.11.8"
)

val sparkVersion = "2.0.1"
lazy val dependencies = Seq(
  libraryDependencies += "log4j" % "log4j" % "1.2.17",
  libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.12",
  libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  libraryDependencies += "org.mockito" % "mockito-all" % "2.0.2-beta" % "test",
  libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion,
  libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion,
  libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion,
  libraryDependencies += "com.databricks" %% "spark-csv" % "1.3.0"
)

lazy val root = (project in file("."))
	.settings(settings : _*)
	.settings(dependencies)
						
