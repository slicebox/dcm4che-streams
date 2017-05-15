import de.heikoseeberger.sbtheader.license.Apache2_0

name := "dcm4che-streams"
version := "0.2"
organization := "se.nimsa"
scalaVersion := "2.12.2"
crossScalaVersions := Seq("2.11.8", "2.12.2")
scalacOptions := Seq("-encoding", "UTF-8", "-Xlint", "-deprecation", "-unchecked", "-feature", "-target:jvm-1.8")

// define the project

lazy val root = (project in file(".")).enablePlugins(GitBranchPrompt)

// repos

resolvers ++= Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "dcm4che Repository" at "http://www.dcm4che.org/maven2/")

// deps

libraryDependencies ++= {
  val akkaVersion = "2.4.17"
  Seq(
    "com.typesafe.akka" %% "akka-stream" % akkaVersion,
    "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
    "org.slf4j" % "slf4j-simple" % "1.7.22",
    "org.dcm4che" % "dcm4che-core" % "3.3.8" % "provided",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test"
  )
}

updateOptions := updateOptions.value.withCachedResolution(true)

// for automatic license stub generation

headers := Map("scala" -> Apache2_0("2017", "Lars Edenbrandt"))

// publish
publishMavenStyle := true

publishArtifact in Test := false

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomIncludeRepository := { _ => false }

pomExtra := (
  <url>https://github.com/slicebox/dcm4che-streams</url>
    <licenses>
      <license>
        <name>Apache-2.0</name>
        <url>https://opensource.org/licenses/Apache-2.0</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:slicebox/dcm4che-streams.git</url>
      <connection>scm:git:git@github.com:slicebox/dcm4che-streams.git</connection>
    </scm>
    <developers>
      <developer>
        <id>KarlSjostrand</id>
        <name>Karl Sjöstrand</name>
        <url>https://github.com/KarlSjostrand</url>
      </developer>
    </developers>)