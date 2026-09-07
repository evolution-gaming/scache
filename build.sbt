import Dependencies.*
import com.typesafe.tools.mima.core.*

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
  versionPolicyIntention := Compatibility.BinaryCompatible,
  versionPolicyIgnored ++= Seq(
    // add libraries here that are known to be binary compatible, like:
    // "com.evolutiongaming" %% "smetrics",
  ),
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "scache-root",
    publish / skip := true,
    publishArtifact := false,
  )
  .aggregate(`cache-adt`, scache, benchmark)

lazy val `cache-adt` = (project in file("cache-adt"))
  .settings(commonSettings)
  .settings(
    name := "cache-adt",
    description := "Directive ADT for scache",
  )

lazy val scache = (project in file("scache"))
  .settings(commonSettings)
  .settings(
    name := "scache",
    description := "Cache in Scala with cats-effect",
    // TODO remove once v7.0.0 is released, from then on the previous version is 7.0.0
    // and none of these are breaks any more.
    // 6.0.2 -> 7.0.0 breaks, reviewed in https://github.com/evolution-gaming/scache/pull/369
    mimaBinaryIssueFilters ++= Seq(
      // Concurrent/Temporal widened to Async
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("com.evolution.scache.Cache.loading"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("com.evolution.scache.Cache.expiring"),
      // only reported on Scala 3, where private[scache] members are emitted as static methods
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("com.evolution.scache.ExpiringCache.of"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("com.evolution.scache.SerialMap.of"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("com.evolution.scache.SerialMap.apply"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("com.evolution.scache.SerialMap#Apply.this"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("com.evolution.scache.SerialMap#Apply.F"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("com.evolution.scache.SerialMap#Apply.of$extension"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("com.evolution.scache.SerialMap#Apply.equals$extension"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("com.evolution.scache.SerialMap#Apply.hashCode$extension"),
      // ExpiringCache.apply takes EntryMap instead of Ref[F, EntryRefs]
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("com.evolution.scache.ExpiringCache.apply"),
      // ExpiringCache.Config gained loadingTimeout
      ProblemFilters.exclude[DirectMissingMethodProblem]("com.evolution.scache.ExpiringCache#Config.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("com.evolution.scache.ExpiringCache#Config.copy"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("com.evolution.scache.ExpiringCache#Config.apply"),
      ProblemFilters.exclude[IncompatibleSignatureProblem]("com.evolution.scache.ExpiringCache#Config.unapply"),
      // internals, LoadingCache state moved to MapRef
      ProblemFilters.exclude[MissingClassProblem]("com.evolution.scache.LoadingCache$EntryRefs$"),
      ProblemFilters.exclude[MissingClassProblem]("com.evolution.scache.ExpiringCache$MapOps"),
      ProblemFilters.exclude[MissingClassProblem]("com.evolution.scache.ExpiringCache$MapOps$"),
    ),
    libraryDependencies ++= Seq(
      Cats.core,
      Cats.effect,
      `cats-helper`,
      smetrics,
      scalatest % Test,
    ),
  )
  .dependsOn(`cache-adt`)

lazy val benchmark = (project in file("benchmark"))
  .enablePlugins(JmhPlugin)
  .settings(commonSettings)
  .settings(
    name := "scache-benchmark",
    description := "JMH benchmarks for scache",
    publish / skip := true,
    publishArtifact := false,
    versionPolicyCheck / skip := true,
    versionPolicyReportDependencyIssues / skip := true,
    coverageEnabled := false,
  )
  .dependsOn(scache)

addCommandAlias("fmt", "scalafmtRepo")
addCommandAlias("check", "+all versionPolicyCheck Compile/doc scalafmtCheckRepo")
addCommandAlias("build", "all test package")
