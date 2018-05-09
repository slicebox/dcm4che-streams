name := "dcm4che-streams"
version := "0.7-SNAPSHOT"
organization := "se.nimsa"
scalaVersion := "2.12.4"
scalacOptions := Seq("-encoding", "UTF-8", "-Xlint", "-deprecation", "-unchecked", "-feature", "-target:jvm-1.8")
scalacOptions in (Compile, doc) ++= Seq(
  "-no-link-warnings" // Suppresses problems with Scaladoc @throws links
)

// repos

resolvers ++= Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "dcm4che Repository" at "https://www.dcm4che.org/maven2/")

// deps

libraryDependencies ++= {
  val akkaVersion = "2.5.6"
  Seq(
    "com.typesafe.akka" %% "akka-stream" % akkaVersion,
    "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
    "org.slf4j" % "slf4j-simple" % "1.7.25",
    "org.dcm4che" % "dcm4che-core" % "3.3.8" % "provided",
    "org.scalatest" %% "scalatest" % "3.0.4" % "test",
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test"
  )
}

updateOptions := updateOptions.value.withCachedResolution(true)

// for automatic license stub generation

organizationName := "Lars Edenbrandt"
startYear := Some(2017)
licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt"))

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

pomExtra :=
  <url>https://github.com/slicebox/dcm4che-streams</url>
  <developers>
    <developer>
      <id>KarlSjostrand</id>
      <name>Karl Sjöstrand</name>
      <url>https://github.com/KarlSjostrand</url>
    </developer>
  </developers>