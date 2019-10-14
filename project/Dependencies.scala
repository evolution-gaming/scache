import sbt._

object Dependencies {

  val scalatest        = "org.scalatest"       %% "scalatest"      % "3.0.8"
  val `cats-helper`    = "com.evolutiongaming" %% "cats-helper"    % "1.0.4"
  val smetrics         = "com.evolutiongaming" %% "smetrics"       % "0.0.6"
  val `kind-projector` = "org.typelevel"        % "kind-projector" % "0.10.3"

  object Cats {
    private val version = "2.0.0"
    val core   = "org.typelevel" %% "cats-core"   % version
    val effect = "org.typelevel" %% "cats-effect" % "2.0.0"
  }
}