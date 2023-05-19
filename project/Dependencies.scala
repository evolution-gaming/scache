import sbt._

object Dependencies {

  val scalatest        = "org.scalatest"       %% "scalatest"          % "3.2.3"
  val `cats-helper`    = "com.evolutiongaming" %% "cats-helper"        % "2.8.0"
  val smetrics         = "com.evolutiongaming" %% "smetrics"           % "0.3.6"
  val `kind-projector` = "org.typelevel"        % "kind-projector"     % "0.13.2"
  val betterMonadicFor = "com.olegpy"          %% "better-monadic-for" % "0.3.1"

  object Cats {
    val core   = "org.typelevel" %% "cats-core"   % "2.8.0"
    val effect = "org.typelevel" %% "cats-effect" % "2.5.5"
  }
}
