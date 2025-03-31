import sbt.*

object Dependencies {

  val scalatest = "org.scalatest" %% "scalatest" % "3.2.19"
  val `cats-helper` = "com.evolutiongaming" %% "cats-helper" % "3.11.3"
  val smetrics = "com.evolutiongaming" %% "smetrics" % "2.2.0"
  val `kind-projector` = "org.typelevel" % "kind-projector" % "0.13.3"
  val betterMonadicFor = "com.olegpy" %% "better-monadic-for" % "0.3.1"

  object Cats {
    val core = "org.typelevel" %% "cats-core" % "2.13.0"
    val effect = "org.typelevel" %% "cats-effect" % "3.6.0"
  }
}
