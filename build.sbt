ThisBuild / version := "0.1-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.12"
ThisBuild / organization := "pl.epsilondeltalimit"

lazy val sparkVersion = "3.0.1" //todo: upgrade Spark to recent version

lazy val tag_pop_sedd = (project in file("."))
  .settings(
    name := "TagPopSEDD",
    assembly / mainClass := Some("pl.epsilondeltalimit.analyzer.StackExchangeDataDumpAnalyzerSingle"),
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,


  )

