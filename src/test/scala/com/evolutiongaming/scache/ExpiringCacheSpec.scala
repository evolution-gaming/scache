package com.evolutiongaming.scache

import cats.Monad
import cats.effect.{Concurrent, IO, Timer}
import cats.implicits._
import com.evolutiongaming.scache.IOSuite._
import org.scalatest.{AsyncFunSuite, Matchers}

import scala.concurrent.duration._

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

  test(s"refresh after write") {
    refreshAfterWrite[IO].run()
  }

  private def expireRecords[F[_] : Concurrent : Timer] = {

    ExpiringCache.of1[F, Int, Int](10.millis).use { cache =>

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

  private def notExpireUsedRecords[F[_] : Concurrent : Timer] = {
    ExpiringCache.of1[F, Int, Int](10.millis).use { cache =>
      val sleep = Timer[F].sleep(3.millis)
      for {
        value0 <- cache.put(0, 0)
        value1 <- cache.put(1, 1)
        _      <- sleep
        _      <- cache.get(0)
        _      <- sleep
        _      <- cache.get(0)
        _      <- sleep
        _      <- cache.get(0)
        _      <- sleep
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


  private def notExceedMaxSize[F[_] : Concurrent : Timer] = {
    ExpiringCache.of1[F, Int, Int](expireAfter = 100.millis, maxSize = 10.some).use { cache =>

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

  private def refreshAfterWrite[F[_] : Concurrent] = {
    ().pure[F]
  }

  object Retry {

    def apply[F[_] : Monad : Timer, A](delay: FiniteDuration, times: Int)(fa: F[Option[A]]): F[Option[A]] = {
      0.tailRecM[F, Option[A]] { round =>
        for {
          a <- fa
          r <- a.fold {
            if (round >= times) none[A].asRight[Int].pure[F]
            else for {
              _ <- Timer[F].sleep(delay)
            } yield {
              (round + 1).asLeft[Option[A]]
            }
          } { value =>
            value.some.asRight[Int].pure[F]
          }
        } yield r
      }
    }
  }
}