import sbt._

object Dependencies {

  val scalatest        = "org.scalatest"       %% "scalatest"      % "3.2.13"
  val `cats-helper`    = "com.evolutiongaming" %% "cats-helper"    % "3.1.1"
  val smetrics         = "com.evolutiongaming" %% "smetrics"       % "1.0.5"
  val `kind-projector` = "org.typelevel"        % "kind-projector" % "0.13.2"

  object Cats {
    val core   = "org.typelevel" %% "cats-core"   % "2.8.0"
    val effect = "org.typelevel" %% "cats-effect" % "3.3.14"
  }
}
