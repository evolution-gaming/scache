import Dependencies.*

def crossSettings[T](scalaVersion: String, if3: T, if2: T): T = {
  scalaVersion match {
    case version if version.startsWith("3") => if3
    case _ => if2
  }
}

lazy val commonSettings = Seq(
  organization := "com.evolution",
  homepage := Some(uri("https://github.com/evolution-gaming/scache")),
  startYear := Some(2019),
  organizationName := "Evolution",
  organizationHomepage := Some(uri("https://evolution.com")),
  scalaVersion := crossScalaVersions.value.head,
  crossScalaVersions := Seq("2.13.18", "3.3.8"),
  scalacOptsFailOnWarn := crossSettings(
    scalaVersion.value,
    if3 = Some(false),
    if2 = Some(true),
  ),
  scalacOptions ++= crossSettings(
    scalaVersion.value,
    if3 = Seq(
      "-Ykind-projector:underscores",
      "-language:implicitConversions",
      "-source:future",
      // improve error messages:
      "-explain",
      "-explain-types",
    ),
    if2 = Seq(
      "-Xsource:3",
      "-P:kind-projector:underscore-placeholders",
    ),
  ),
  // -Wnonunit-statement (added in sbt-scalac-opts-plugin 0.1.0) flags ScalaTest's
  // `x shouldEqual y` used as a non-final statement; too noisy to fix across specs right now.
  Test / scalacOptions -= "-Wnonunit-statement",
  // Under sbt 2 the compile cache can skip re-creating scoverage's data directory after `clean`.
  // When a module's instrumented code runs inside another module's forked test JVM (before that
  // module's own tests, if any, have run), scoverage.Invoker then crashes writing measurements to
  // the missing directory. Ensure it always exists after compile whenever coverage is enabled.
  Compile / compile := Def.uncached {
    val compiled = (Compile / compile).value
    if (coverageEnabled.value) {
      val _ = (crossTarget.value / "scoverage-data").mkdirs()
    }
    compiled
  },
  libraryDependencies ++= crossSettings(
    scalaVersion.value,
    if3 = Nil,
    if2 = Seq(
      compilerPlugin(betterMonadicFor),
      compilerPlugin(`kind-projector`.cross(CrossVersion.full)),
    ),
  ),
  autoAPIMappings := true,
  licenses := Seq(("MIT", uri("https://opensource.org/licenses/MIT"))),
  Test / publishArtifact := false,
  publishTo := Some(Resolver.evolutionReleases),
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "scache-root",
    publish / skip := true,
    publishArtifact := false,
  )
  .aggregate(`cache-adt`, scache)

lazy val `cache-adt` = (project in file("cache-adt"))
  .settings(commonSettings)
  .settings(
    name := "cache-adt",
    description := "Directive ADT for scache",
    versionPolicyIntention := Compatibility.BinaryCompatible,
  )

lazy val scache = (project in file("scache"))
  .settings(commonSettings)
  .settings(
    name := "scache",
    description := "Cache in Scala with cats-effect",
    versionPolicyIntention := Compatibility.None,
    libraryDependencies ++= Seq(
      Cats.core,
      Cats.effect,
      `cats-helper`,
      smetrics,
      scalatest % Test,
    ),
  )
  .dependsOn(`cache-adt`)

addCommandAlias("fmt", "+scalafmtRepo")
addCommandAlias("check", "+all versionPolicyCheck Compile/doc scalafmtCheckRepo")
addCommandAlias("build", "all test package")
