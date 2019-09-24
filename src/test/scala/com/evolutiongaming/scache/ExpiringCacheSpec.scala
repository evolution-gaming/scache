package com.evolutiongaming.scache

import cats.{Monad, Parallel}
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, IO, Sync, Timer}
import cats.implicits._
import com.evolutiongaming.scache.IOSuite._
import org.scalatest.{AsyncFunSuite, Matchers}

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class ExpiringCacheSpec extends AsyncFunSuite with Matchers {

  test(s"expire entries") {
    expireRecords[IO].run()
  }

  test("not expire used entries") {
    notExpireUsedRecords[IO].run()
  }

  test(s"not exceed max size") {
    notExceedMaxSize[IO].run()
  }

  test(s"refresh periodically") {
    refreshPeriodically[IO].run()
  }

  test("refresh does not touch entries") {
    refreshDoesNotTouch[IO].run()
  }

  test("refresh fails") {
    refreshFails[IO].run()
  }

  private def expireRecords[F[_] : Concurrent : Timer : Parallel] = {

    ExpiringCache.of[F, Int, Int](100.millis).use { cache =>
      for {
        release <- Deferred[F, Unit]
        value   <- cache.put(0, 0, release.complete(()))
        value   <- value
        _       <- Sync[F].delay { value shouldEqual none }
        value   <- cache.get(0)
        _       <- Sync[F].delay { value shouldEqual 0.some }
        _       <- release.get
        value   <- cache.get(0)
        _       <- Sync[F].delay { value shouldEqual none }
      } yield {}
    }
  }

  private def notExpireUsedRecords[F[_] : Concurrent : Timer : Parallel] = {
    ExpiringCache.of[F, Int, Int](50.millis).use { cache =>
      val touch = for {
        _ <- Timer[F].sleep(10.millis)
        _ <- cache.get(0)
      } yield {}
      for {
        release <- Ref[F].of(false)
        value   <- cache.put(0, 0, release.set(true))
        value   <- value
        _       <- Sync[F].delay { value shouldEqual none }
        value   <- cache.put(1, 1)
        value   <- value
        _       <- Sync[F].delay { value shouldEqual none }
        _       <- List.fill(6)(touch).foldMapM(identity)
        value   <- cache.get(0)
        _       <- Sync[F].delay { value shouldEqual 0.some }
        value   <- cache.get(1)
        _       <- Sync[F].delay { value shouldEqual none }
        release <- release.get
        _       <- Sync[F].delay { release shouldEqual false}
      } yield {}
    }
  }


  private def notExceedMaxSize[F[_] : Concurrent : Timer : Parallel] = {
    ExpiringCache.of[F, Int, Int](expireAfter = 100.millis, maxSize = 10.some).use { cache =>
      for {
        release <- Deferred[F, Unit]
        _       <- cache.put(0, 0, release.complete(()))
        _       <- (1 until 10).toList.foldMapM { n => cache.put(n, n).void }
        value   <- cache.get(0)
        _       <- Sync[F].delay { value shouldEqual 0.some }
        _       <- cache.put(10, 10)
        _       <- release.get
      } yield {}
    }
  }

  private def refreshPeriodically[F[_] : Concurrent : Timer : Parallel] = {

    val value = (key: Int) => key.pure[F]
    val refresh = ExpiringCache.Refresh(100.millis, value)

    ExpiringCache.of[F, Int, Int](1.minute, refresh = refresh.some).use { cache =>

      def retryUntilRefreshed(key: Int, original: Int) = {
        Retry(10.millis, 100) {
          for {
            value <- cache.get(key)
          } yield {
            value.filter(_ != original)
          }
        }
      }

      for {
        value <- cache.put(0, 1)
        value <- value
        _     <- Sync[F].delay { value shouldEqual none }
        value <- cache.get(0)
        _     <- Sync[F].delay { value shouldEqual 1.some }
        value <- retryUntilRefreshed(0, 1)
        _     <- Sync[F].delay { value shouldEqual 0.some }
      } yield {}
    }
  }

  private def refreshDoesNotTouch[F[_] : Concurrent : Timer : Parallel] = {
    val value = (key: Int) => key.pure[F]
    val refresh = ExpiringCache.Refresh(100.millis, value)

    ExpiringCache.of[F, Int, Int](100.millis, refresh = refresh.some).use { cache =>

      def retryUntilRefreshed(key: Int, original: Int) = {
        Retry(10.millis, 100) {
          for {
            value <- cache.get(key)
          } yield {
            value.filter(_ != original)
          }
        }
      }

      for {
        released <- Ref[F].of(false)
        release  <- Deferred[F, Unit]
        value    <- cache.put(0, 1, released.set(true) *> release.complete(()))
        value    <- value
        _        <- Sync[F].delay { value shouldEqual none }
        value    <- cache.get(0)
        _        <- Sync[F].delay { value shouldEqual 1.some }
        value    <- retryUntilRefreshed(0, 1)
        released <- released.get
        _        <- Sync[F].delay { released shouldEqual false}
        _        <- Sync[F].delay { value shouldEqual 0.some }
        _        <- release.get
      } yield {}
    }
  }

  private def refreshFails[F[_] : Concurrent : Timer : Parallel] = {

    def valueOf(ref: Ref[F, Int]) = {
      (_: Int) => {
        for {
          n <- ref.modify { n => (n + 1, n) }
          v <- if (n == 0) TestError.raiseError[F, Int] else 1.pure[F]
        } yield v
      }
    }

    for {
      ref     <- Ref[F].of(0)
      value    = valueOf(ref)
      refresh  = ExpiringCache.Refresh(50.millis, value)
      result  <- ExpiringCache.of[F, Int, Int](1.minute, refresh = refresh.some).use { cache =>

        def retryUntilRefreshed(key: Int, original: Int) = {
          Retry(10.millis, 100) {
            for {
              value <- cache.get(key)
            } yield {
              value.filter(_ != original)
            }
          }
        }

        for {
          value <- cache.put(0, 0)
          value <- value
          _     <- Sync[F].delay { value shouldEqual none }
          value <- cache.get(0)
          _     <- Sync[F].delay { value shouldEqual 0.some }
          value <- retryUntilRefreshed(0, 0)
          _     <- Sync[F].delay { value shouldEqual 1.some }
          value <- ref.get
          _     <- Sync[F].delay { value should be >= 1 }
        } yield {}
      }
    } yield result
  }

  object Retry {

    def apply[F[_] : Monad : Timer, A](
      delay: FiniteDuration,
      times: Int)(
      fa: F[Option[A]]
    ): F[Option[A]] = {

      def retry(round: Int) = {
        if (round >= times) none[A].asRight[Int].pure[F]
        else for {
          _ <- Timer[F].sleep(delay)
        } yield {
          (round + 1).asLeft[Option[A]]
        }
      }

      0.tailRecM[F, Option[A]] { round =>
        for {
          a <- fa
          r <- a.fold { retry(round) } { _.some.asRight[Int].pure[F] }
        } yield r
      }
    }
  }

  case object TestError extends RuntimeException with NoStackTrace
}