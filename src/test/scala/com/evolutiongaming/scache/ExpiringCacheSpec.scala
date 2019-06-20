package com.evolutiongaming.scache

import cats.Monad
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, IO, Timer}
import cats.implicits._
import cats.temp.par._
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

  private def expireRecords[F[_] : Concurrent : Timer : Par] = {

    ExpiringCache.of[F, Int, Int](10.millis).use { cache =>

      def retryUntilExpired(key: Int) = {
        Retry(10.millis, 100) {
          for {
            values <- cache.values
          } yield {
            val value = values.get(key)
            value.fold { ().some } { _ => none[Unit] }
          }
        }
      }

      for {
        value0 <- cache.put(0, 0)
        value1 <- cache.get(0)
        value2 <- retryUntilExpired(0)
        value3 <- cache.get(0)
      } yield {
        value0 shouldEqual none
        value1 shouldEqual 0.some
        value2 shouldEqual ().some
        value3 shouldEqual none[Int]
      }
    }
  }

  private def notExpireUsedRecords[F[_] : Concurrent : Timer : Par] = {
    ExpiringCache.of[F, Int, Int](50.millis).use { cache =>
      val touch = for {
        _ <- Timer[F].sleep(10.millis)
        _ <- cache.get(0)
      } yield {}
      for {
        value0 <- cache.put(0, 0)
        value1 <- cache.put(1, 1)
        _      <- List.fill(6)(touch).foldMapM(identity)
        value2 <- cache.get(0)
        value3 <- cache.get(1)
      } yield {
        value0 shouldEqual none
        value1 shouldEqual none
        value2 shouldEqual 0.some
        value3 shouldEqual none
      }
    }
  }


  private def notExceedMaxSize[F[_] : Concurrent : Timer : Par] = {
    ExpiringCache.of[F, Int, Int](expireAfter = 100.millis, maxSize = 10.some).use { cache =>

      def retryUntilCleaned(key: Int) = {
        Retry(10.millis, 100) {
          for {
            values <- cache.values
          } yield {
            val value = values.get(key)
            value.fold { ().some } { _ => none[Unit] }
          }
        }
      }

      for {
        _      <- (0 until 10).toList.foldMapM { n => cache.put(n, n).void }
        value0 <- cache.get(0)
        _      <- cache.put(10, 10)
        value1 <- retryUntilCleaned(0)
      } yield {
        value0 shouldEqual 0.some
        value1 shouldEqual ().some
      }
    }
  }

  private def refreshPeriodically[F[_] : Concurrent : Timer : Par] = {

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
        value0 <- cache.put(0, 1)
        value1 <- cache.get(0)
        value2 <- retryUntilRefreshed(0, 1)
      } yield {
        value0 shouldEqual none
        value1 shouldEqual 1.some
        value2 shouldEqual 0.some
      }
    }
  }

  private def refreshDoesNotTouch[F[_] : Concurrent : Timer : Par] = {
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

      def retryUntilExpired(key: Int) = {
        Retry(10.millis, 100) {
          for {
            values <- cache.values // TODO replace with getNotTouch and others
          } yield {
            val value = values.get(key)
            value.fold { ().some } { _ => none[Unit] }
          }
        }
      }

      for {
        value0 <- cache.put(0, 1)
        value1 <- cache.get(0)
        value2 <- retryUntilRefreshed(0, 1)
        value3 <- retryUntilExpired(0)
      } yield {
        value0 shouldEqual none
        value1 shouldEqual 1.some
        value2 shouldEqual 0.some
        value3 shouldEqual ().some
      }
    }
  }

  private def refreshFails[F[_] : Concurrent : Timer : Par] = {

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
          value0 <- cache.put(0, 0)
          value1 <- cache.get(0)
          value2 <- retryUntilRefreshed(0, 0)
          value4 <- ref.get
        } yield {
          value0 shouldEqual none
          value1 shouldEqual 0.some
          value2 shouldEqual 1.some
          value4 should be >= 1
        }
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