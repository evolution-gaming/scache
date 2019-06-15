import sbt._

object Dependencies {

  val scalatest     = "org.scalatest"       %% "scalatest"    % "3.0.7"
  val `cats-par`    = "io.chrisdavenport"   %% "cats-par"     % "0.2.1"
  val `cats-helper` = "com.evolutiongaming" %% "cats-helper"  % "0.0.12"

  object Cats {
    private val version = "1.6.1"
    val core   = "org.typelevel" %% "cats-core"   % version
    val effect = "org.typelevel" %% "cats-effect" % "1.3.1"
  }
}