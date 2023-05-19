package com.evolution.scache

import cats.effect.*
import cats.syntax.all.*
import com.evolution.scache.IOSuite.*
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

class ExpiringCacheSpec extends AsyncFunSuite with Matchers {

  test(s"expire entries") {
    expireRecords[IO].run()
  }

  test(s"expire created entries") {
    `expire created entries`[IO].run()
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

  test("refresh removes entry") {
    `refresh removes entry`[IO].run()
  }

  private def expireRecords[F[_] : Async] = {

    ExpiringCache.of[F, Int, Int](ExpiringCache.Config(expireAfterRead = 100.millis)).use { cache =>
      for {
        release <- Deferred[F, Unit]
        value   <- cache.put(0, 0, release.complete(()).void)
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

  private def `expire created entries`[F[_] : Async] = {
    val  config = ExpiringCache.Config[F, Int, Int](
      expireAfterRead = 1.minute,
      expireAfterWrite = 150.millis.some)
    ExpiringCache.of[F, Int, Int](config).use { cache =>
      for {
        release <- Deferred[F, Unit]
        _       <- cache.put(0, 0, release.complete(()).void)
        _       <- Temporal[F].sleep(50.millis)
        value   <- cache.get(0)
        _       <- Sync[F].delay { value shouldEqual 0.some }
        _       <- release.get
        value   <- cache.get(0)
        _       <- Sync[F].delay { value shouldEqual none }
      } yield {}
    }
  }

  private def notExpireUsedRecords[F[_] : Async] = {
    ExpiringCache.of[F, Int, Int](ExpiringCache.Config(50.millis)).use { cache =>
      val touch = for {
        _ <- Temporal[F].sleep(10.millis)
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


  private def notExceedMaxSize[F[_] : Async] = {
    val config = ExpiringCache.Config[F, Int, Int](
      expireAfterRead = 100.millis,
      expireAfterWrite = 100.millis.some,
      maxSize = 10.some)
    ExpiringCache.of(config).use { cache =>
      for {
        release <- Deferred[F, Unit]
        _       <- cache.put(0, 0, release.complete(()).void)
        _       <- (1 until 10).toList.foldMapM { n => cache.put(n, n).void }
        value   <- cache.get(0)
        _       <- Sync[F].delay { value shouldEqual 0.some }
        _       <- cache.put(10, 10)
        _       <- release.get
      } yield {}
    }
  }

  private def refreshPeriodically[F[_] : Async] = {
    val refresh = ExpiringCache.Refresh[Int](100.millis) { _.some.pure[F] }
    val config = ExpiringCache.Config(
      expireAfterRead = 1.minute,
      expireAfterWrite = 1.minute.some,
      refresh = refresh.some)
    ExpiringCache.of[F, Int, Int](config).use { cache =>

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

  private def refreshDoesNotTouch[F[_] : Async] = {
    val refresh = ExpiringCache.Refresh[Int](100.millis) { _.some.pure[F] }

    val config = ExpiringCache.Config(
      expireAfterRead = 100.millis,
      refresh = refresh.some)

    ExpiringCache.of[F, Int, Int](config).use { cache =>

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
        value    <- cache.put(0, 1, released.set(true) *> release.complete(()).void)
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

  private def refreshFails[F[_] : Async] = {

    def valueOf(ref: Ref[F, Int]) = {
      (_: Int) => {
        for {
          n <- ref.modify { n => (n + 1, n) }
          v <- if (n == 0) TestError.raiseError[F, Int] else 1.pure[F]
        } yield {
          v.some
        }
      }
    }

    for {
      ref     <- Ref[F].of(0)
      value    = valueOf(ref)
      refresh  = ExpiringCache.Refresh(50.millis, value)
      config   = ExpiringCache.Config(
        expireAfterRead = 1.minute,
        expireAfterWrite = 1.minute.some,
        refresh = refresh.some)
      result  <- ExpiringCache.of(config).use { cache =>

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

  def `refresh removes entry`[F[_] : Async] = {
    val refresh = ExpiringCache.Refresh[Int](100.millis) { _ => none[Int].pure[F] }

    val config = ExpiringCache.Config(
      expireAfterRead = 100.millis,
      refresh = refresh.some)

    ExpiringCache.of[F, Int, Int](config).use { cache =>

      def retryUntilNone(key: Int) = {
        0.tailRecM[F, Option[Int]] { round =>
          for {
            a <- cache.get(key)
            r <- a match {
              case Some(a) =>
                if (round >= 100) {
                  a.some.asRight[Int].pure[F]
                } else {
                  for {
                    _ <- Temporal[F].sleep(10.millis)
                  } yield {
                    (round + 1).asLeft[Option[Int]]
                  }
                }
              case None => none.asRight[Int].pure[F]
            }
          } yield r
        }
      }

      for {
        released <- Ref[F].of(false)
        release  <- Deferred[F, Unit]
        value    <- cache.put(0, 1, released.set(true) *> release.complete(()).void)
        value    <- value
        _        <- Sync[F].delay { value shouldEqual none }
        value    <- cache.get(0)
        _        <- Sync[F].delay { value shouldEqual 1.some }
        value    <- retryUntilNone(0)
        _        <- Temporal[F].sleep(10.millis) // Account to `release` being performed asynchronously
        released <- released.get
        _        <- Sync[F].delay { released shouldEqual true}
        _        <- Sync[F].delay { value shouldEqual none }
        _        <- release.get
      } yield {}
    }.replicateA_(20)
  }


  object Retry {

    def apply[F[_] : Temporal, A](
      delay: FiniteDuration,
      times: Int)(
      fa: F[Option[A]]
    ): F[Option[A]] = {

      def retry(round: Int) = {
        if (round >= times) none[A].asRight[Int].pure[F]
        else for {
          _ <- Temporal[F].sleep(delay)
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