import Dependencies._

name := "scache"

organization := "com.evolution"

homepage := Some(new URL("http://github.com/evolution-gaming/scache"))

startYear := Some(2019)

organizationName := "Evolution Gaming"

organizationHomepage := Some(url("http://evolutiongaming.com"))

scalaVersion := crossScalaVersions.value.head

crossScalaVersions := Seq("2.13.8", "2.12.17")

libraryDependencies ++= Seq(
  compilerPlugin(betterMonadicFor),
  compilerPlugin(`kind-projector` cross CrossVersion.full)
)

libraryDependencies ++= Seq(
  Cats.core,
  Cats.effect,
  `cats-helper`,
  smetrics,
  scalatest % Test
)

autoAPIMappings := true

// autoAPIMappings was not enabled for CE2, so we do this manually
// this is only needed to support `[[...]]` syntax for scaladoc
Compile / apiMappings := {

  val log = streams.value.log
  val mappings = (Compile / apiMappings).value

  val moduleId = s"${Cats.effect.name}_${scalaBinaryVersion.value}"
  val libraryFile = (Compile / dependencyClasspath).value find { libraryFile =>
    libraryFile.metadata.get(moduleID.key).map(_.name) == Some(moduleId)
  }
  val libraryUrl = url("https://typelevel.org/cats-effect/api/2.x/")

  if (libraryFile.isEmpty) {
    log.warn(s"Could not find `$moduleId` in dependencies, `doc` task might fail with an error")
  }

  // if library file is not found, we just ignore it and change nothing
  libraryFile.fold(mappings) { libraryFile =>
    mappings + (libraryFile.data -> libraryUrl)
  }

}

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

developers              := List(
    Developer(
      "t3hnar",
      "Yaroslav Klymko",
      "yklymko@evolution.com",
      url("https://github.com/t3hnar"),
    )
  )

enablePlugins(GitVersioning)
