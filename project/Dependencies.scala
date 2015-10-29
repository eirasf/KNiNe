import sbt._
import sbtassembly.Plugin._


object Dependencies {
  val resolutionRepos = Seq(
    "Akka Repository" at "http://repo.akka.io/releases/",
    "Spray Repository" at "http://repo.spray.cc/",
    "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
  )

  object V {
    val spark     = "1.4.1"
    val specs2    = "1.13" // -> "1.13" when we bump to Scala 2.10.0
    val guava     = "11.0.1"
    val hamcrest  = "1.3"
    val scalatest = "2.0"
    val junit     = "4.11"
    val parser    = "1.5.6"

    // Add versions for your additional libraries here...
  }

  object Libraries {
    val sparkCore    = "org.apache.spark"           %% "spark-core"            % V.spark        % "provided"
    val sparkMllib   = "org.apache.spark"           %% "spark-mllib"           % V.spark        % "provided"
    val sparkSql     = "org.apache.spark"           %% "spark-sql"             % V.spark        % "provided"
    val scalaTest    = "org.scalatest"              %% "scalatest"             % V.scalatest    % "test"

    // Java
    // Add additional libraries from mvnrepository.com (SBT syntax) here...
    val parser       = "com.univocity"      % "univocity-parsers"     % V.parser

    val junit        = "junit"              % "junit"                 % V.junit     % "test"
    val specs2       = "org.specs2"         % "specs2_2.10"           % V.specs2    % "test"
    val guava        = "com.google.guava"   % "guava"                 % V.guava     % "test"
    val hamcrest     = "org.hamcrest"       % "hamcrest-all"          % V.hamcrest  % "test"
  }
}