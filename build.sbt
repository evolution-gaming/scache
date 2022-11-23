import Dependencies._

name := "scache"

organization := "com.evolution"

homepage := Some(new URL("http://github.com/evolution-gaming/scache"))

startYear := Some(2019)

organizationName := "Evolution Gaming"

organizationHomepage := Some(url("http://evolutiongaming.com"))

scalaVersion := crossScalaVersions.value.head

crossScalaVersions := Seq("2.13.8", "2.12.17")

libraryDependencies ++= crossSettings(
  scalaVersion.value,
  if3 = Nil,
  if2 = Seq(compilerPlugin(`kind-projector` cross CrossVersion.full)),
)

scalacOptions ++= crossSettings(
  scalaVersion.value,
  if3 = Seq("-Ykind-projector:underscores", "-language:implicitConversions"),
  if2 = Seq("-Xsource:3", "-P:kind-projector:underscore-placeholders"),
)

libraryDependencies ++= Seq(
  Cats.core,
  Cats.effect,
  `cats-helper`,
  smetrics,
  scalatest % Test
)

licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT")))

description             := "Cache in Scala with cats-effect"

sonatypeCredentialHost := "s01.oss.sonatype.org"

sonatypeRepository := "https://s01.oss.sonatype.org/service/local"

Test / publishArtifact  := false

scmInfo                 := Some(
  ScmInfo(
    url("https://github.com/evolution-gaming/scache"),
    "git@github.com:evolution-gaming/scache.git",
  ),
)

enablePlugins(GitVersioning)
