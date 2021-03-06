name := "ReactiveCouchbase-core"

organization := "org.reactivecouchbase"

isSnapshot := true

version := "0.5-SNAPSHOT"

scalaVersion := "2.11.8"

// Allow debugging during tests
fork in Test := false

unmanagedResourceDirectories in Test <+= baseDirectory(_ / "target/web/public/test")

resolvers ++= Seq(
  Resolver.typesafeRepo("releases"), Resolver.bintrayRepo("scalaz", "releases")
)

libraryDependencies ++= Seq(
  "com.couchbase.client" % "couchbase-client" % "1.4.11",
  "com.typesafe.akka" %% "akka-actor" % "2.4.4" cross CrossVersion.binary,
  "com.typesafe.play" %% "play-iteratees" % "2.5.3" cross CrossVersion.binary,
  "com.typesafe.play" %% "play-json" % "2.5.3" cross CrossVersion.binary,
  "com.typesafe.play" %% "play-specs2" % "2.5.3" % "test" cross CrossVersion.binary,
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "com.ning" % "async-http-client" % "1.9.38",
  "com.typesafe" % "config" % "1.3.0",
  "com.codahale.metrics" % "metrics-core" % "3.0.2"
)

publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }
pomExtra := (
  <url>http://reactivecouchbase.org</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:ReactiveCouchbase/ReactiveCouchbase-core.git</url>
      <connection>scm:git:git@github.com:ReactiveCouchbase/ReactiveCouchbase-core.git</connection>
    </scm>
    <developers>
      <developer>
        <id>mathieu.ancelin</id>
        <name>Mathieu ANCELIN</name>
        <url>https://github.com/mathieuancelin</url>
      </developer>
    </developers>)
