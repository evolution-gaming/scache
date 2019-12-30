import sbt._

object Dependencies {

  val scalatest        = "org.scalatest"       %% "scalatest"      % "3.1.0"
  val `cats-helper`    = "com.evolutiongaming" %% "cats-helper"    % "1.4.0"
  val smetrics         = "com.evolutiongaming" %% "smetrics"       % "0.0.8"
  val `kind-projector` = "org.typelevel"        % "kind-projector" % "0.11.0"

  object Cats {
    private val version = "2.0.0"
    val core   = "org.typelevel" %% "cats-core"   % version
    val effect = "org.typelevel" %% "cats-effect" % version
  }
}
