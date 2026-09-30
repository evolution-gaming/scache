package com.evolution.scache

import cats.effect.*
import cats.effect.syntax.all.*
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

  test("expire stuck loads") {
    `expire stuck loads`[IO].run()
  }

  test("loading timeout does not expire loaded values") {
    `loading timeout does not expire loaded values`[IO].run()
  }

  test("clear gives up on stuck loads") {
    `clear gives up on stuck loads`[IO].run()
  }

  test("release gives up on stuck loads") {
    `release gives up on stuck loads`[IO].run()
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

  private def expireRecords[F[_]: Async] = {

    ExpiringCache.of[F, Int, Int](ExpiringCache.Config[F, Int, Int](expireAfterRead = 100.millis)).use { cache =>
      for {
        release <- Deferred[F, Unit]
        value <- cache.put(0, 0, release.complete(()).void)
        value <- value
        _ <- Sync[F].delay { value shouldEqual none }
        value <- cache.get(0)
        _ <- Sync[F].delay { value shouldEqual 0.some }
        _ <- release.get
        value <- cache.get(0)
        _ <- Sync[F].delay { value shouldEqual none }
      } yield {}
    }
  }

  private def `expire created entries`[F[_]: Async] = {
    val config = ExpiringCache.Config[F, Int, Int](
      expireAfterRead = 1.minute,
      expireAfterWrite = 150.millis.some,
    )
    ExpiringCache.of[F, Int, Int](config).use { cache =>
      for {
        release <- Deferred[F, Unit]
        _ <- cache.put(0, 0, release.complete(()).void)
        _ <- Temporal[F].sleep(50.millis)
        value <- cache.get(0)
        _ <- Sync[F].delay { value shouldEqual 0.some }
        _ <- release.get
        value <- cache.get(0)
        _ <- Sync[F].delay { value shouldEqual none }
      } yield {}
    }
  }

  private def notExpireUsedRecords[F[_]: Async] = {
    ExpiringCache.of[F, Int, Int](ExpiringCache.Config[F, Int, Int](50.millis)).use { cache =>
      val touch = for {
        _ <- Temporal[F].sleep(10.millis)
        _ <- cache.get(0)
      } yield {}
      for {
        release <- Ref[F].of(false)
        value <- cache.put(0, 0, release.set(true))
        value <- value
        _ <- Sync[F].delay { value shouldEqual none }
        released <- Deferred[F, Unit]
        value <- cache.put(1, 1, released.complete(()).void)
        value <- value
        _ <- Sync[F].delay { value shouldEqual none }
        _ <- List.fill(6)(touch).foldMapM(identity)
        // The cleanup routine owns the moment of the eviction, and key 1 must not be read while it
        // is being waited out, as a read would refresh it. Its release callback signals the
        // eviction instead, with key 0 kept in use by the very same waiting.
        _ <- Temporal[F].timeout((touch *> released.tryGet).iterateUntil { _.isDefined }, 5.seconds)
        value <- cache.get(1)
        _ <- Sync[F].delay { value shouldEqual none }
        value <- cache.get(0)
        _ <- Sync[F].delay { value shouldEqual 0.some }
        release <- release.get
        _ <- Sync[F].delay { release shouldEqual false }
      } yield {}
    }
  }

  private def notExceedMaxSize[F[_]: Async] = {
    val config = ExpiringCache.Config[F, Int, Int](
      expireAfterRead = 100.millis,
      expireAfterWrite = 100.millis.some,
      maxSize = 10.some,
    )
    ExpiringCache.of(config).use { cache =>
      for {
        release <- Deferred[F, Unit]
        _ <- cache.put(0, 0, release.complete(()).void)
        _ <- (1 until 10).toList.foldMapM { n => cache.put(n, n).void }
        value <- cache.get(0)
        _ <- Sync[F].delay { value shouldEqual 0.some }
        _ <- cache.put(10, 10)
        _ <- release.get
      } yield {}
    }
  }

  private def `expire stuck loads`[F[_]: Async] = {
    val config = ExpiringCache.Config[F, Int, Int](
      expireAfterRead = 1.minute,
      loadingTimeout = 100.millis.some,
    )
    ExpiringCache.of[F, Int, Int](config).use { cache =>
      for {
        started <- Deferred[F, Unit]
        // The load is held by a gate rather than by `never`, so that a failed assertion below ends
        // the test instead of hanging the release of the cache.
        gate <- Deferred[F, Unit]
        loader <- cache.getOrUpdate(0) { started.complete(()) *> gate.get.as(0) }.attempt.start
        _ <- started.get
        result <- {
          for {
            waiter <- cache.getOrUpdate(0) { 1.pure[F] }.attempt.start
            outcome <- Temporal[F].timeout(waiter.joinWithNever, 5.seconds)
            _ <- Sync[F].delay { outcome should matchPattern { case Left(ExpiredError) => } }
            value <- cache.get(0)
            _ <- Sync[F].delay { value shouldEqual none }
            value <- Temporal[F].timeout(cache.getOrUpdate(0) { 2.pure[F] }, 5.seconds)
            _ <- Sync[F].delay { value shouldEqual 2 }
          } yield {}
        }.guarantee { gate.complete(()) *> loader.join.void }
      } yield result
    }
  }

  private def `loading timeout does not expire loaded values`[F[_]: Async] = {
    val config = ExpiringCache.Config[F, Int, Int](
      expireAfterRead = 1.minute,
      loadingTimeout = 100.millis.some,
    )
    ExpiringCache.of[F, Int, Int](config).use { cache =>
      for {
        value <- cache.getOrUpdate(0) { 0.pure[F] }
        _ <- Sync[F].delay { value shouldEqual 0 }
        _ <- Temporal[F].sleep(500.millis)
        value <- cache.get(0)
        _ <- Sync[F].delay { value shouldEqual 0.some }
      } yield {}
    }
  }

  private def `clear gives up on stuck loads`[F[_]: Async] = {
    val config = ExpiringCache.Config[F, Int, Int](
      expireAfterRead = 1.minute,
      loadingTimeout = 100.millis.some,
    )
    ExpiringCache.of[F, Int, Int](config).use { cache =>
      for {
        started <- Deferred[F, Unit]
        gate <- Deferred[F, Unit]
        released <- Deferred[F, Unit]
        loader <- cache
          .getOrUpdate1(0) { started.complete(()) *> gate.get.as((0, 0, released.complete(()).void.some)) }
          .attempt
          .start
        _ <- started.get
        waiter <- cache.getOrUpdate(0) { 1.pure[F] }.attempt.start
        result <- {
          for {
            _ <- Temporal[F].timeout(cache.clear.flatten, 2.seconds)
            outcome <- Temporal[F].timeout(waiter.joinWithNever, 2.seconds)
            _ <- Sync[F].delay { outcome should matchPattern { case Left(ExpiredError) => } }
            _ <- gate.complete(())
            outcome <- Temporal[F].timeout(loader.joinWithNever, 2.seconds)
            _ <- Sync[F].delay { outcome should matchPattern { case Left(ExpiredError) => } }
            _ <- Temporal[F].timeout(released.get, 2.seconds)
            value <- cache.get(0)
            _ <- Sync[F].delay { value shouldEqual none }
          } yield {}
        }.guarantee { gate.complete(()) *> loader.join.void }
      } yield result
    }
  }

  private def `release gives up on stuck loads`[F[_]: Async] = {
    val config = ExpiringCache.Config[F, Int, Int](
      expireAfterRead = 1.minute,
      loadingTimeout = 100.millis.some,
    )
    for {
      started <- Deferred[F, Unit]
      gate <- Deferred[F, Unit]
      released <- Deferred[F, Unit]
      (cache, release) <- ExpiringCache.of[F, Int, Int](config).allocated
      loader <- cache
        .getOrUpdate1(0) { started.complete(()) *> gate.get.as((0, 0, released.complete(()).void.some)) }
        .attempt
        .start
      _ <- started.get
      // The release is uncancelable, hence run in a fiber of its own with the timeout on the join,
      // so that a release that hangs fails the test rather than blocking it.
      releasing <- release.start
      result <- {
        for {
          _ <- Temporal[F].timeout(releasing.joinWithNever, 2.seconds)
          _ <- gate.complete(())
          outcome <- Temporal[F].timeout(loader.joinWithNever, 2.seconds)
          _ <- Sync[F].delay { outcome should matchPattern { case Left(ExpiredError) => } }
          _ <- Temporal[F].timeout(released.get, 2.seconds)
        } yield {}
      }.guarantee { gate.complete(()) *> loader.join.void *> releasing.join.void }
    } yield result
  }

  private def refreshPeriodically[F[_]: Async] = {
    val refresh = ExpiringCache.Refresh[Int](100.millis) { _.some.pure[F] }
    val config = ExpiringCache.Config(
      expireAfterRead = 1.minute,
      expireAfterWrite = 1.minute.some,
      refresh = refresh.some,
    )
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
        _ <- Sync[F].delay { value shouldEqual none }
        value <- cache.get(0)
        _ <- Sync[F].delay { value shouldEqual 1.some }
        value <- retryUntilRefreshed(0, 1)
        _ <- Sync[F].delay { value shouldEqual 0.some }
      } yield {}
    }
  }

  private def refreshDoesNotTouch[F[_]: Async] = {
    val refresh = ExpiringCache.Refresh[Int](100.millis) { _.some.pure[F] }

    val config = ExpiringCache.Config(
      expireAfterRead = 100.millis,
      refresh = refresh.some,
    )

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
        release <- Deferred[F, Unit]
        value <- cache.put(0, 1, released.set(true) *> release.complete(()).void)
        value <- value
        _ <- Sync[F].delay { value shouldEqual none }
        value <- cache.get(0)
        _ <- Sync[F].delay { value shouldEqual 1.some }
        value <- retryUntilRefreshed(0, 1)
        released <- released.get
        _ <- Sync[F].delay { released shouldEqual false }
        _ <- Sync[F].delay { value shouldEqual 0.some }
        _ <- release.get
      } yield {}
    }
  }

  private def refreshFails[F[_]: Async] = {

    def valueOf(ref: Ref[F, Int]) = {
      (_: Int) =>
        {
          for {
            n <- ref.modify { n => (n + 1, n) }
            v <- if (n == 0) TestError.raiseError[F, Int] else 1.pure[F]
          } yield {
            v.some
          }
        }
    }

    for {
      ref <- Ref[F].of(0)
      value = valueOf(ref)
      refresh = ExpiringCache.Refresh(50.millis, value)
      config = ExpiringCache.Config(
        expireAfterRead = 1.minute,
        expireAfterWrite = 1.minute.some,
        refresh = refresh.some,
      )
      result <- ExpiringCache.of(config).use { cache =>
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
          _ <- Sync[F].delay { value shouldEqual none }
          value <- cache.get(0)
          _ <- Sync[F].delay { value shouldEqual 0.some }
          value <- retryUntilRefreshed(0, 0)
          _ <- Sync[F].delay { value shouldEqual 1.some }
          value <- ref.get
          _ <- Sync[F].delay { value should be >= 1 }
        } yield {}
      }
    } yield result
  }

  def `refresh removes entry`[F[_]: Async] = {
    val refresh = ExpiringCache.Refresh[Int](100.millis) { _ => none[Int].pure[F] }

    val config = ExpiringCache.Config(
      expireAfterRead = 100.millis,
      refresh = refresh.some,
    )

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
        released <- Deferred[F, Boolean]
        release <- Deferred[F, Unit]
        value <- cache.put(0, 1, released.complete(true) *> release.complete(()).void)
        value <- value
        _ <- Sync[F].delay { value shouldEqual none }
        value <- cache.get(0)
        _ <- Sync[F].delay { value shouldEqual 1.some }
        value <- retryUntilNone(0)
        released <- released.get
        _ <- Sync[F].delay { released shouldEqual true }
        _ <- Sync[F].delay { value shouldEqual none }
        _ <- release.get
      } yield {}
    }
  }

  object Retry {

    def apply[F[_]: Temporal, A](
      delay: FiniteDuration,
      times: Int,
    )(
      fa: F[Option[A]],
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
