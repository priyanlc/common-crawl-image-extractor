ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.12"

fork in Test := true
javaOptions ++= Seq("-Xms8G", "-Xmx8G", "-XX:MaxPermSize=4048M", "-XX:+CMSClassUnloadingEnabled")

scalacOptions += "-target:jvm-1.8"

initialize := {
  val _ = initialize.value // run the previous initialization
  val required = "1.8"
  val current  = sys.props("java.specification.version")
  assert(current == required, s"Unsupported JDK: java.specification.version $current != $required")
}

lazy val root = (project in file("."))
  .settings(
    name := "common-crawl-image-extractor",

    // Assembly merge strategy
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x if x.endsWith("MANIFEST.MF") => MergeStrategy.discard
      case x if x.endsWith(".sf") => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )

libraryDependencies ++= Seq(
  // Spark SQL for DataFrame operations
  "org.apache.spark" %% "spark-sql" % "3.3.2",

  // ScalaTest for testing
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,

  // Mockito for mocking in ScalaTest
//  "org.scalatestplus" %% "scalatestplus-mockito" % "1.0.0-M2" % Test,

  "com.holdenkarau" %% "spark-testing-base" % "3.3.1_1.4.0" % Test,

//  "org.mockito" %% "mockito-scala" % "1.17.29" % Test,

  "org.scalatestplus" %% "mockito-4-11" % "3.2.17.0" % Test


)
