import Common.Library.implicits
import Common.*
import sbt.project

name    := "AlgoSEDD"
version := "0.1"

updateSbtClassifiers / useCoursier := true

lazy val root = (project in file("."))
  .settings(Common.settings(ProjectVersion(0, 1)))
  .settings(Common.assemblyConf)
  .settings(
    libraryDependencies ++= Library.spark    % Provided,
    libraryDependencies ++= Library.sparkXml % Provided,
    libraryDependencies ++= Library.scopt,
    libraryDependencies ++= Library.logging,
    libraryDependencies ++= Library.pureConfig,
    libraryDependencies ++= Library.guava,
    libraryDependencies ++= Library.dep,
    libraryDependencies ++= Library.sparkTests % Test,
    libraryDependencies ++= Library.scalaTest  % Test,
    libraryDependencies ++= Library.scalaMock  % Test
  )
  .settings(
    Test / packageBin / publishArtifact := true
  )
