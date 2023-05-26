package com.evolution.scache

import cats.effect.unsafe.implicits.global
import cats.effect.{Clock, IO}
import com.evolutiongaming.catshelper.MeasureDuration
import org.scalactic.source.Position
import org.scalatest.Succeeded
import org.scalatest.enablers.Retrying
import org.scalatest.time.Span

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

object IOSuite {
  val Timeout: FiniteDuration = 5.seconds

  implicit val executor: ExecutionContextExecutor = ExecutionContext.global
  implicit val measureDuration: MeasureDuration[IO] = MeasureDuration.fromClock(Clock[IO])

  // Allows to use Eventually methods with IO
  implicit def ioRetrying[T]: Retrying[IO[T]] = new Retrying[IO[T]] {
    override def retry(timeout: Span, interval: Span, pos: Position)(f: => IO[T]): IO[T] =
      IO.fromFuture(
        IO(Retrying.retryingNatureOfFutureT[T](executor).retry(timeout, interval, pos)(f.unsafeToFuture())),
      )
  }

  def runIO[A](io: IO[A], timeout: FiniteDuration = Timeout): Future[Succeeded.type] = {
    io.timeout(timeout).as(Succeeded).unsafeToFuture()
  }

  implicit class IOOps[A](val self: IO[A]) extends AnyVal {
    def run(timeout: FiniteDuration = Timeout): Future[Succeeded.type] = runIO(self, timeout)
  }
}