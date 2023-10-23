ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.12"

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
  "org.apache.spark" %% "spark-sql" % "3.5.0",

  // ScalaTest for testing
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,

  // Mockito for mocking in ScalaTest
  "org.scalatestplus" %% "scalatestplus-mockito" % "1.0.0-M2" % Test
)
