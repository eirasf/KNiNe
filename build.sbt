name := "knine"

version := "0.1"

organization := "es.udc"

scalaVersion := "2.10.6"

resolvers ++= Seq(
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/",
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
)

publishMavenStyle := true

sparkPackageName := "udc/graph-knine"

sparkVersion := "1.6.0"

sparkComponents += "mllib"
