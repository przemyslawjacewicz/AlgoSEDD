import sbt.*
import sbt.Keys.*
import sbtassembly.AssemblyKeys.*
import sbtassembly.AssemblyPlugin.autoImport.{MergeStrategy, PathList, assemblyMergeStrategy, assemblyOption}

object Common {

  val projectScalaVersion = "2.12.15"
  val projectOrganization = "pl.epsilondeltalimit"

  val assemblyConf = Seq(
    assembly / test := {},
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case x: Any => MergeStrategy.first
    }
  )

  /**
   * Get common basic settings for a module
   *
   * @param projectVersion version to set
   * @return basic settings for a module with provided version
   */
  def settings(projectVersion: ProjectVersion) = Seq(
    organization := projectOrganization,
    scalaVersion := projectScalaVersion,
    ThisBuild / useCoursier := false, // Disabling coursier fixes the problem with java.lang.NoClassDefFoundError: scala/xml while
    // publishing child modules: https://github.com/sbt/sbt/issues/4995

    run / fork := true,
    Test / parallelExecution := false,
    Test / fork := false,
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD"),
    javaOptions ++= Seq("-Dlog4j.debug=true", "-Dlog4j.configuration=log4j.properties"),
    outputStrategy := Some(StdoutOutput),
    isSnapshot := projectVersion.snapshot,
    version := projectVersion.fullVersion,
    resolvers += DefaultMavenRepository,
    resolvers += Resolver.mavenLocal
  ) ++ {
    if (projectVersion.snapshot)
      Seq(
        assembly / test := ((): Unit),
        publishConfiguration := publishConfiguration.value.withOverwrite(true)
      )
    else
      Seq()
  }

  /**
   * Library for code dependencies
   */
  object Library {

    /**
     * The following implicits will enable the scoping of seq of dependencies rather than single dependency
     *
     * @param sq dependencies
     */
    implicit class implicits(sq: Seq[ModuleID]) {
      def %(conf: Configuration): Seq[ModuleID] = sq.map(_ % conf)

      def exclude(org: String, name: String): Seq[ModuleID] =
        sq.map(_.exclude(org, name))

      def excludeAll(rules: ExclusionRule*): Seq[ModuleID] =
        sq.map(_.excludeAll(rules: _*))

    }

    private val sparkOrg: String = "org.apache.spark"
    private val sparkVersion = "3.2.1"

    lazy val spark = Seq(
      sparkOrg %% "spark-core" % sparkVersion,
      sparkOrg %% "spark-sql" % sparkVersion,
      sparkOrg %% "spark-catalyst" % sparkVersion
    )

    lazy val sparkXml = Seq(
      "com.databricks"       %% "spark-xml"  % "0.16.0"
    )

    lazy val sparkTests = Seq(
      sparkOrg %% "spark-sql" % sparkVersion classifier "tests",
      sparkOrg %% "spark-core" % sparkVersion classifier "tests",
      sparkOrg %% "spark-catalyst" % sparkVersion classifier "tests"
    )

    lazy val logging = Seq(
      "log4j" % "log4j" % "1.2.17"
    )

    lazy val scopt = Seq(
      "com.github.scopt" %% "scopt" % "4.0.1"
    )

    lazy val scalaTest = Seq(
      "org.scalactic" %% "scalactic" % "3.2.15",
      "org.scalatest" %% "scalatest" % "3.2.15"
    )

    lazy val scalaMock = Seq(
      "org.scalamock" %% "scalamock" % "5.2.0"
    )

    lazy val pureConfig = Seq(
      "com.github.pureconfig" %% "pureconfig" % "0.17.1"
    )

    lazy val guava = Seq(
      "com.google.guava"      % "guava"      % "23.0"
    )

    lazy val dep = Seq(
      "pl.epsilondeltalimit" %% "dep"        % "0.1"
    )

  }
}
