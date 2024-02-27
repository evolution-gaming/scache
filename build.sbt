import sbt.librarymanagement.For3Use2_13
import Dependencies._
import org.scoverage.coveralls.Imports.CoverallsKeys._

def crossSettings[T](scalaVersion: String, if3: T, if2: T) = {
  scalaVersion match {
    case version if version.startsWith("3") => if3
    case _                                  => if2
  }
}

name := "scache"

organization := "com.evolution"

homepage := Some(new URL("http://github.com/evolution-gaming/scache"))

startYear := Some(2019)

organizationName := "Evolution Gaming"

organizationHomepage := Some(url("http://evolutiongaming.com"))
coverageExcludedFiles := ".*CacheOpsCompat.*"

scalaVersion := crossScalaVersions.value.head

crossScalaVersions := Seq("2.13.11", "3.3.0", "2.12.19")

scalacOptsFailOnWarn := crossSettings(
  scalaVersion.value,
  if3 = Some(false),
  if2 = Some(true)
)

libraryDependencies ++= crossSettings(
  scalaVersion.value,
  if3 = Nil,
  if2 = Seq(
    compilerPlugin(betterMonadicFor),
    compilerPlugin(`kind-projector` cross CrossVersion.full)
  )
)

scalacOptions ++= crossSettings(
  scalaVersion.value,
  if3 = Seq(
    "-Ykind-projector:underscores",
    "-language:implicitConversions",
    "-source:future"
  ),
  if2 = Seq("-Xsource:3", "-P:kind-projector:underscore-placeholders")
)

libraryDependencies ++= Seq(
  Cats.core,
  Cats.effect,
  `cats-helper`,
  smetrics,
  scalatest % Test
)

autoAPIMappings := true

licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT")))

description := "Cache in Scala with cats-effect"

sonatypeCredentialHost := "s01.oss.sonatype.org"

sonatypeRepository := "https://s01.oss.sonatype.org/service/local"

Test / publishArtifact := false

scmInfo := Some(
  ScmInfo(
    url("https://github.com/evolution-gaming/scache"),
    "git@github.com:evolution-gaming/scache.git"
  )
)

developers := List(
  Developer(
    "t3hnar",
    "Yaroslav Klymko",
    "yklymko@evolution.com",
    url("https://github.com/t3hnar")
  )
)

enablePlugins(GitVersioning)
