ThisBuild / version := "0.1-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.15"
ThisBuild / organization := "pl.epsilondeltalimit"

lazy val sparkVersion = "3.2.1"

lazy val root = (project in file("."))
  .settings(
    name := "AlgoSEDD",
    //    assembly / mainClass := Some("pl.epsilondeltalimit.analyzer.StackExchangeDataDumpAnalyzerSingle"),
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    libraryDependencies += "com.databricks" %% "spark-xml" % "0.17.0",
    libraryDependencies += "com.google.guava" % "guava" % "23.0",
    libraryDependencies += "pl.epsilondeltalimit" %% "dep" % "0.1",
  )

