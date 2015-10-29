import sbt._
import Keys._

object SparkGeoInferProjectBuild extends Build {

  import Dependencies._
  import BuildSettings._

  override lazy val settings = super.settings :+ {
    // Configure prompt to show current project
    shellPrompt := { s => Project.extract(s).currentProject.id + " > " }
  }

  conflictManager := ConflictManager.strict

  // Define our project, with basic project information and library dependencies
  lazy val project = Project("strath-graph", file("."))
    .settings(buildSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        Libraries.sparkCore,
        Libraries.sparkMllib,
        Libraries.sparkSql,
        Libraries.guava,
        Libraries.specs2,
        Libraries.hamcrest,
        Libraries.scalaTest,
        Libraries.junit,
        Libraries.parser
        // Add your additional libraries here (comma-separated)...
      )
    )
}
