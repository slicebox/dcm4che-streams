import de.heikoseeberger.sbtheader.license.Apache2_0

name := "dcm4che-streams"
version := "1.0-SNAPSHOT"
organization := "se.nimsa"
scalaVersion := "2.12.1"
crossScalaVersions := Seq("2.11.8", "2.12.1")
scalacOptions := Seq("-encoding", "UTF-8", "-Xlint", "-deprecation", "-unchecked", "-feature", "-target:jvm-1.8")

// define the project

lazy val root = (project in file(".")).enablePlugins(GitBranchPrompt)

// repos

resolvers ++= Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/")

// deps

libraryDependencies ++= {
  val akkaVersion = "2.4.16"
  Seq(
    "com.typesafe.akka" %% "akka-stream" % akkaVersion,
    "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
    "org.slf4j" % "slf4j-simple" % "1.7.22",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test"
  )
}

updateOptions := updateOptions.value.withCachedResolution(true)

// for automatic license stub generation

headers := Map("scala" -> Apache2_0("2017", "Lars Edenbrandt"))
