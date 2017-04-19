organization := "io.rgruener"
name := "enron-scio"
description := "Data pipeline for Enron email analysis"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.11.8"
scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked")
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

resolvers ++= Seq(
  "Concurrent Maven Repo" at "http://conjars.org/repo",
  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"
)

val scioVersion = "0.3.0-beta3"
val slf4jVersion = "1.7.13"

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % scioVersion,
  "com.spotify" %% "scio-extra" % scioVersion,
  "org.slf4j" % "slf4j-simple" % slf4jVersion,
  "com.spotify" %% "scio-test" % scioVersion % "test"
)
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)