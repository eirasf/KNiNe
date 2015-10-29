import sbt._
import Keys._

object BuildSettings {

  // Basic settings for our app
  lazy val basicSettings = Seq[Setting[_]](
    organization  := "org.udc",
    version       := "0.3.0",
    description   := "Graph algorithms on top of spark",
    scalaVersion  := "2.10.4",
    scalacOptions := Seq("-deprecation", "-encoding", "utf8"),
    resolvers     ++= Dependencies.resolutionRepos
  )

  // sbt-assembly settings for building a fat jar
  import sbtassembly.Plugin._
  import AssemblyKeys._
  lazy val sbtAssemblySettings = assemblySettings ++ Seq(

    // Slightly cleaner jar name
    jarName in assembly := {
      name.value + "-" + version.value + ".jar"
    },

    // Drop these jars
    excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
      val excludes = Set(
        "jsp-api-2.1-6.1.14.jar",
        "jsp-2.1-6.1.14.jar",
        "jasper-compiler-5.5.12.jar",
        "commons-beanutils-core-1.8.0.jar",
        "commons-beanutils-1.7.0.jar",
        "servlet-api-2.5-20081211.jar",
        "servlet-api-2.5.jar"
      )
      cp filter { jar => excludes(jar.data.getName) }
    },

    mergeStrategy in assembly <<= (mergeStrategy in assembly) {
      (old) => {
        // case "project.clj" => MergeStrategy.discard // Leiningen build files
        case x if x.startsWith("META-INF") => MergeStrategy.discard // Bumf
        case x if x.endsWith(".html") => MergeStrategy.discard // More bumf
        case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last // For Log$Logger.class
        case x => old(x)
      }
    }
  )

  lazy val buildSettings = basicSettings ++ sbtAssemblySettings
}
