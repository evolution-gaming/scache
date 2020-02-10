package com.evolutiongaming.scache


import cats.effect.concurrent.Ref
import cats.effect.{IO, Resource}
import cats.implicits._
import com.evolutiongaming.scache.IOSuite._
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers


class CacheFencedTest extends AsyncFunSuite with Matchers {

  private val cache = Cache.loading[IO, Int, Int]().withFence

  test(s"get succeeds after cache is released") {
    val result = for {
      cache <- cache.use(_.pure[IO])
      a     <- cache.get(0)
      _      = a shouldEqual none
    } yield {}
    result.run()
  }

  test(s"put succeeds after cache is released") {
    val result = for {
      cache <- cache.use(_.pure[IO])
      a     <- cache.put(0, 0).flatten
      _      = a shouldEqual none
    } yield {}
    result.run()
  }

  test(s"put releasable fails after cache is released") {
    val result = for {
      cache <- cache.use(_.pure[IO])
      a     <- cache.put(0, 0, ().pure[IO]).flatten.attempt
      _      = a shouldEqual CacheReleasedError.asLeft
    } yield {}
    result.run()
  }

  test(s"size succeeds after cache is released") {
    val result = for {
      cache <- cache.use { cache => cache.put(0, 0).flatten as cache }
      a     <- cache.size
      _      = a shouldEqual 0
    } yield {}
    result.run()
  }

  test(s"remove succeeds after cache is released") {
    val result = for {
      cache <- cache.use(_.pure[IO])
      a     <- cache.remove(0).flatten
      _      = a shouldEqual none
    } yield {}
    result.run()
  }

  test(s"clear succeeds after cache is released") {
    val result = for {
      cache <- cache.use(_.pure[IO])
      _     <- cache.clear.flatten
    } yield {}
    result.run()
  }

  test(s"getOrUpdate succeeds after cache is released") {
    val result = for {
      cache <- cache.use(_.pure[IO])
      a     <- cache.getOrUpdate(0)(1.pure[IO])
      _      = a shouldEqual 1
    } yield {}
    result.run()
  }

  test(s"getOrUpdateReleasable fails after cache is released") {
    val result = for {
      cache <- cache.use(_.pure[IO])
      a     <- cache.getOrUpdateReleasable(0)(Releasable[IO].pure(1).pure[IO]).attempt
      _      = a shouldEqual CacheReleasedError.asLeft
    } yield {}
    result.run()
  }

  test("release happens once") {

    def cache(ref: Ref[IO, Int]) = {
      val cache = for {
        _     <- Resource.make(().pure[IO]) { _ => ref.update(_ + 1) }
        cache <- Cache.loading[IO, Int, Int]().withFence
      } yield cache
      cache.withFence
    }

    val result = for {
      ref <- Ref[IO].of(0)
      ab  <- cache(ref).allocated
      (_, release) = ab
      _   <- release
      _   <- release
      a   <- ref.get
      _    = a shouldEqual 1
    } yield {}
    result.run()
  }
}
