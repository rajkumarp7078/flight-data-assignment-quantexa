scalaVersion := "2.13.8"

name := "flight-data-assignment-quantexa"
organization := "Quantexa Flight Analytics"
version := "1.0"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "3.2.2",
  "org.apache.spark" %% "spark-sql" % "3.2.2",
  "com.typesafe" % "config" % "1.4.2")
