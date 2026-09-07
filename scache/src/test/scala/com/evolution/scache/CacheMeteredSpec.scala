package com.evolution.scache

import cats.effect.{IO, Ref}
import com.evolution.scache.IOSuite.*
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

class CacheMeteredSpec extends AsyncFunSuite with Matchers {

  private def sizeRecorder(sizes: Ref[IO, List[Int]]): CacheMetrics[IO] = new CacheMetrics[IO] {
    def get(hit: Boolean): IO[Unit] = IO.unit
    def load(time: FiniteDuration, success: Boolean): IO[Unit] = IO.unit
    def life(time: FiniteDuration): IO[Unit] = IO.unit
    def put: IO[Unit] = IO.unit
    def modify(entryExisted: Boolean, directive: CacheMetrics.Directive): IO[Unit] = IO.unit
    def size(size: Int): IO[Unit] = sizes.update(size :: _)
    def size(latency: FiniteDuration): IO[Unit] = IO.unit
    def values(latency: FiniteDuration): IO[Unit] = IO.unit
    def keys(latency: FiniteDuration): IO[Unit] = IO.unit
    def clear(latency: FiniteDuration): IO[Unit] = IO.unit
    def foldMap(latency: FiniteDuration): IO[Unit] = IO.unit
  }

  test("size is reported on schedule and withdrawn on release") {
    val result = for {
      sizes <- Ref.of[IO, List[Int]](Nil)
      cache = Cache.loading[IO, Int, Int].flatMap { cache => CacheMetered(cache, sizeRecorder(sizes), 10.millis) }
      _ <- cache.use { cache =>
        for {
          _ <- cache.put(0, 0)
          _ <- cache.put(1, 1)
          _ <- (IO.sleep(5.millis) *> sizes.get).iterateUntil(_.contains(2)).timeout(1.second)
        } yield {}
      }
      sizes <- sizes.get
      _ = sizes.head shouldEqual 0
      _ = sizes should contain(2)
    } yield {}
    result.run()
  }
}
