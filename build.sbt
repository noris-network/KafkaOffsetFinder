val scalaLibVersion = "2.11.7"
val slf4jVersion = "1.7.7"

lazy val dependencies = Seq(
  "org.apache.kafka" % "kafka-clients" % "0.10.2.1",
  "org.slf4j" % "slf4j-api" % slf4jVersion
)

lazy val buildVersions = Seq(
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  scalaVersion := scalaLibVersion,
  scalacOptions ++= Seq("-target:jvm-1.8"),
  scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-Xlint")
)


lazy val KafkaOffsetFinder = project.in(file("."))
  .settings(buildVersions: _*)
  .settings(
    name := "OffsetFinder",
    organization := "de.noris",
    libraryDependencies ++= dependencies,
    // Run command in a new JVM, required to set JVM options
    fork := true
  )
