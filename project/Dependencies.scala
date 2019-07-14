import sbt._

object Dependencies {

  val scalatest     = "org.scalatest"       %% "scalatest"   % "3.0.8"
  val `cats-par`    = "io.chrisdavenport"   %% "cats-par"    % "0.2.1"
  val `cats-helper` = "com.evolutiongaming" %% "cats-helper" % "0.0.18"
  val smetrics      = "com.evolutiongaming" %% "smetrics"    % "0.0.3"

  object Cats {
    private val version = "1.6.1"
    val core   = "org.typelevel" %% "cats-core"   % version
    val effect = "org.typelevel" %% "cats-effect" % "1.3.1"
  }
}