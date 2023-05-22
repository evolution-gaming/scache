package com.evolution.scache

import cats.Monad
import cats.effect.implicits.*
import cats.effect.*
import cats.syntax.all.*
import com.evolutiongaming.catshelper.CatsHelper.*
import com.evolution.scache.IOSuite.*
import org.scalatest.Assertion
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

class CacheSpec extends AsyncFunSuite with Matchers {
  import CacheSpec.*

  private val expiringCache = Cache.expiring[IO, Int, Int](
    config = ExpiringCache.Config[IO, Int, Int](expireAfterRead = 1.minute),
    partitions = None,
  )

  for {
    (name, cache0) <- List(
      ("default"               , Cache.loading[IO, Int, Int]),
      ("no partitions"         , LoadingCache.of(LoadingCache.EntryRefs.empty[IO, Int, Int])),
      ("expiring"              , expiringCache),
      ("expiring no partitions", ExpiringCache.of[IO, Int, Int](ExpiringCache.Config(expireAfterRead = 1.minute))))
  } yield {

    val cacheAndMetrics =
      for {
        cache   <- cache0
        metrics <- CacheMetricsProbe.of.toResource
        cache   <- cache.withMetrics(metrics)
        cache   <- cache.withFence
      } yield (cache, metrics)

    def check(testName: String)(assertions: (Cache[IO, Int, Int], CacheMetricsProbe) => IO[Any]): Unit = test(testName) {
      cacheAndMetrics.use(assertions.tupled).run()
    }

    check(s"get: $name") { (cache, metrics) =>
      for {
        value <- cache.get(0)
        _     <- IO { value shouldEqual none[Int] }
        _     <- metrics.expect(metrics.expectedGet(hit = false) -> 1)
      } yield {}
    }

    check(s"get1: $name") { (cache, metrics) =>
      for {
        a <- cache.get1(0)
        _ <- IO { a shouldEqual none[Int] }
        d <- Deferred[IO, Int]
        f <- cache.getOrUpdateEnsure(1) { d.get }
        a <- cache.get1(1)
        _ <- IO { a.map { _.leftMap { _ => () } } shouldEqual ().asLeft.some }
        _ <- d.complete(1)
        _ <- f.joinWithNever
        a <- 0.tailRecM { counter =>
          cache.get1(1).flatMap {
            case Some(Left(_)) if counter <= 10 =>
              IO
                .sleep(1.millis)
                .as {
                  (counter + 1).asLeft[Option[Either[IO[Int], Int]]]
                }

            case a =>
              a
                .asRight[Int]
                .pure[IO]
          }
        }
        _ <- IO { a shouldEqual 1.asRight.some }
        _ <- cache.put(2, 2)
        a <- cache.get1(2)
        _ <- IO { a shouldEqual 2.asRight.some }
        _ <- metrics.expect(
          metrics.expectedGet(hit = false)     -> 2,
          metrics.expectedGet(hit = true)      -> 3,
          metrics.expectedLoad(success = true) -> 1,
          metrics.expectedPut                  -> 1
        )
      } yield {}
    }

    test(s"get succeeds after cache is released: $name") {
      val result = for {
        (cache, metrics) <- cacheAndMetrics.use { _.pure[IO] }
        a                <- cache.get(0)
        _                <- IO { a shouldEqual none[Int] }
        _                <- metrics.expect(metrics.expectedGet(hit = false) -> 1)
      } yield {}
      result.run()
    }

    check(s"getOrElse: $name") { (cache, metrics) =>
      for {
        value <- cache.getOrElse(0, 1.pure[IO])
        _     <- IO { value shouldEqual 1 }
        _     <- cache.put(0, 2)
        value <- cache.getOrElse(0, 1.pure[IO])
        _     <- IO { value shouldEqual 2 }
        _     <- metrics.expect(
          metrics.expectedGet(hit = false) -> 1,
          metrics.expectedPut              -> 1,
          metrics.expectedGet(hit = true)  -> 1
        )
      } yield {}
    }

    test(s"getOrElse succeeds after cache is released: $name") {
      val result = for {
        (cache, metrics) <- cacheAndMetrics.use { _.pure[IO] }
        a                <- cache.getOrElse(0, 1.pure[IO])
        _                <- IO { a shouldEqual 1 }
        _                <- metrics.expect(metrics.expectedGet(hit = false) -> 1)
      } yield {}
      result.run()
    }

    check(s"put: $name") { (cache, metrics) =>
      for {
        value <- cache.put(0, 0)
        value <- value
        _     <- IO { value shouldEqual none }
        value <- cache.get(0)
        _     <- IO { value shouldEqual 0.some }
        value <- cache.put(0, 1)
        value <- value
        _     <- IO { value shouldEqual 0.some }
        value <- cache.put(0, 2)
        value <- value
        _     <- IO { value shouldEqual 1.some }
        _     <- metrics.expect(
          metrics.expectedPut             -> 3,
          metrics.expectedGet(hit = true) -> 1,
          metrics.expectedLife            -> 2
        )
      } yield {}
    }

    test(s"put succeeds after cache is released: $name") {
      val result = for {
        (cache, metrics) <- cacheAndMetrics.use { _.pure[IO] }
        a                <- cache.put(0, 0).flatten
        _                <- IO { a shouldEqual none[Int] }
        _                <- metrics.expect(metrics.expectedPut -> 1)
      } yield {}
      result.run()
    }

    check(s"put releasable: $name") { (cache, metrics) =>
      for {
        release0 <- Deferred[IO, Unit]
        release1 <- Deferred[IO, Unit]
        released <- Ref[IO].of(false)
        value    <- cache.put(0, 0, release0.complete(()) *> release1.get *> released.set(true))
        value    <- value
        _        <- IO { value shouldEqual none }
        value    <- cache.get(0)
        _        <- IO { value shouldEqual 0.some }
        value    <- cache.put(0, 1)
        _        <- release0.get
        _        <- release1.complete(())
        value    <- value
        _        <- IO { value shouldEqual 0.some }
        value    <- released.get
        _        <- IO { value shouldEqual true }
        value    <- cache.put(0, 2)
        value    <- value
        _        <- IO { value shouldEqual 1.some }
        _        <- metrics.expect(
          metrics.expectedPut             -> 3,
          metrics.expectedGet(hit = true) -> 1,
          metrics.expectedLife            -> 2
        )
      } yield {}
    }

    test(s"put releasable fails after cache is released: $name") {
      val result = for {
        (cache, metrics) <- cacheAndMetrics.use { _.pure[IO] }
        a                <- cache.put(0, 0, ().pure[IO]).flatten.attempt
        _                <- IO { a shouldEqual CacheReleasedError.asLeft }
        _                <- metrics.expect()
      } yield {}
      result.run()
    }

    check(s"contains: $name") { (cache, metrics) =>
      for {
        a <- cache.contains(0)
        _ <- IO { a shouldBe false }
        a <- cache.contains(1)
        _ <- IO { a shouldBe false }

        _ <- cache.put(0, 0)

        a <- cache.contains(0)
        _ <- IO { a shouldBe true }
        a <- cache.contains(1)
        _ <- IO { a shouldBe false }

        _ <- cache.put(1, 1)

        a <- cache.contains(0)
        _ <- IO { a shouldBe true }
        a <- cache.contains(1)
        _ <- IO { a shouldBe true }

        _ <- cache.remove(0)

        a <- cache.contains(0)
        _ <- IO { a shouldBe false }
        a <- cache.contains(1)
        _ <- IO { a shouldBe true }

        _ <- cache.clear

        a <- cache.contains(0)
        _ <- IO { a shouldBe false }
        a <- cache.contains(1)
        _ <- IO { a shouldBe false }
        _ <- metrics.expect(
          metrics.expectedPut                  -> 2,
          metrics.expectedLife                 -> 2,
          metrics.expectedClear                -> 1
        )
      } yield {}
    }

    check(s"size: $name") { (cache, metrics) =>
      for {
        size <- cache.size
        _    <- IO { size shouldEqual 0 }
        _    <- cache.put(0, 0)
        size <- cache.size
        _    <- IO { size shouldEqual 1 }
        _    <- cache.put(0, 1)
        size <- cache.size
        _    <- IO { size shouldEqual 1 }
        _    <- cache.put(1, 1)
        size <- cache.size
        _    <- IO { size shouldEqual 2 }
        _    <- cache.remove(0)
        size <- cache.size
        _    <- IO { size shouldEqual 1 }
        _    <- cache.clear
        size <- cache.size
        _    <- IO { size shouldEqual 0 }
        _    <- metrics.expect(
          metrics.expectedSize  -> 6,
          metrics.expectedPut   -> 3,
          metrics.expectedLife  -> 3,
          metrics.expectedClear -> 1
        )
      } yield {}
    }

    test(s"size succeeds after cache is released: $name") {
      val result = for {
        (cache, metrics) <- cacheAndMetrics.use { case (cache, metrics) => cache.put(0, 0).flatten.as((cache, metrics)) }
        a                <- cache.size
        _                <- IO { a shouldEqual 0 }
        _                <- metrics.expect(
          metrics.expectedPut  -> 1,
          metrics.expectedLife -> 1,
          metrics.expectedSize -> 1
        )
      } yield {}
      result.run()
    }

    check(s"remove: $name") { (cache, metrics) =>
      for {
        _     <- cache.put(0, 0)
        value <- cache.remove(0)
        value <- value
        _     <- IO { value shouldEqual 0.some }
        value <- cache.get(0)
        _     <- IO { value shouldEqual none }
        _     <- metrics.expect(
          metrics.expectedPut              -> 1,
          metrics.expectedLife             -> 1,
          metrics.expectedGet(hit = false) -> 1
        )
      } yield {}
    }

    test(s"remove succeeds after cache is released: $name") {
      val result = for {
        (cache, metrics) <- cacheAndMetrics.use { _.pure[IO] }
        a                <- cache.remove(0).flatten
        _                <- IO { a shouldEqual none }
        _                <- metrics.expect()
      } yield {}
      result.run()
    }

    check(s"clear: $name") { (cache, metrics) =>
      for {
        _      <- cache.put(0, 0)
        _      <- cache.put(1, 1)
        _      <- cache.clear
        value0 <- cache.get(0)
        value1 <- cache.get(1)
        _      <- IO { value0 shouldEqual none[Int] }
        _      <- IO { value1 shouldEqual none[Int] }
        _      <- metrics.expect(
          metrics.expectedPut              -> 2,
          metrics.expectedLife             -> 2,
          metrics.expectedClear            -> 1,
          metrics.expectedGet(hit = false) -> 2
        )
      } yield {}
    }

    test(s"clear succeeds after cache is released: $name") {
      val result = for {
        (cache, metrics) <- cacheAndMetrics.use { _.pure[IO] }
        _                <- cache.clear.flatten
        _                <- metrics.expect(metrics.expectedClear -> 1)
      } yield {}
      result.run()
    }

    check(s"clear releasable: $name") { (cache, metrics) =>
      for {
        release   <- Deferred[IO, Unit]
        released0 <- Ref[IO].of(false)
        _         <- cache.put(0, 0, release.get *> released0.set(true))
        _         <- cache.put(1, 1, TestError.raiseError[IO, Unit])
        released1 <- Ref[IO].of(false)
        _         <- cache.getOrUpdate1(2) { (2, 2, (release.get *> released1.set(true)).some).pure[IO] }
        _         <- cache.getOrUpdate1(3) { (3, 3, TestError.raiseError[IO, Unit].some).pure[IO] }
        keys      <- cache.keys
        _         <- IO { keys shouldEqual Set(0, 1, 2, 3) }
        clear     <- cache.clear
        _ <- keys.toList.foldMapM { key =>
          for {
            value    <- cache.get(key)
            _        <- IO { value shouldEqual none[Int] }
          } yield {}
        }
        keys  <- cache.keys
        _     <- IO { keys shouldEqual Set.empty }
        _     <- release.complete(()).start
        _     <- clear
        value <- released0.get
        _     <- IO { value shouldEqual true }
        value <- released1.get
        _     <- IO { value shouldEqual true }
        _     <- metrics.expect(
          metrics.expectedKeys                 -> 2,
          metrics.expectedGet(hit = false)     -> 6,
          metrics.expectedPut                  -> 2,
          metrics.expectedLife                 -> 4,
          metrics.expectedClear                -> 1,
          metrics.expectedLoad(success = true) -> 2,
        )
      } yield {}
    }

    check(s"put & get many: $name") { (cache, metrics) =>

      val values = (0 to 100).toList

      def getAll(cache: Cache[IO, Int, Int]) = {
        values.foldM(List.empty[Option[Int]]) { (result, key) =>
          for {
            value <- cache.get(key)
          } yield {
            value :: result
          }
        }
      }

      for {
        result0 <- getAll(cache)
        _       <- values.foldMapM { key => cache.put(key, key).void }
        result1 <- getAll(cache)
        _       <- IO { result0.flatten.reverse shouldEqual List.empty }
        _       <- IO { result1.flatten.reverse shouldEqual values }
        _       <- metrics.expect(
          metrics.expectedGet(hit = false) -> 101,
          metrics.expectedPut              -> 101,
          metrics.expectedGet(hit = true)  -> 101,
        )
      } yield {}
    }

    check(s"getOrUpdate: $name") { (cache, metrics) =>
      for {
        deferred <- Deferred[IO, Int]
        value0   <- cache.getOrUpdateEnsure(0) { deferred.get }
        value2   <- cache.getOrUpdate(0)(1.pure[IO]).startEnsure
        _        <- deferred.complete(0)
        value0   <- value0.joinWithNever
        value1   <- value2.joinWithNever
        _        <- IO { value0 shouldEqual 0.asRight }
        _        <- IO { value1 shouldEqual 0 }
        _        <- metrics.expect(
          metrics.expectedGet(hit = false)     -> 1,
          metrics.expectedLoad(success = true) -> 1,
          metrics.expectedGet(hit = true)      -> 1
        )
      } yield {}
    }

    test(s"getOrUpdate succeeds after cache is released: $name") {
      val result = for {
        (cache, metrics) <- cacheAndMetrics.use { _.pure[IO] }
        a                <- cache.getOrUpdate(0)(1.pure[IO])
        _                <- IO { a shouldEqual 1 }
        _                <- metrics.expect(
          metrics.expectedGet(hit = false)     -> 1,
          metrics.expectedLoad(success = true) -> 1,
        )
      } yield {}
      result.run()
    }

    check(s"getOrUpdateOpt: $name") { (cache, metrics) =>
      for {
        deferred <- Deferred[IO, Option[Int]]
        value0   <- cache.getOrUpdateOptEnsure(0) { deferred.get }
        value1   <- cache.getOrUpdateOpt(0)(0.some.pure[IO]).startEnsure
        _        <- deferred.complete(none)
        value0   <- value0.joinWithNever
        value1   <- value1.joinWithNever
        _         = value0 shouldEqual none[Int].asRight
        _         = value1 shouldEqual none[Int]
        value    <- cache.getOrUpdateOpt(0)(0.some.pure[IO])
        _         = value shouldEqual 0.some
        _ <- metrics.expect(
          metrics.expectedGet(hit = false)     -> 2,
          metrics.expectedGet(hit = true)      -> 1,
          metrics.expectedLoad(success = true) -> 2,
        )
      } yield {}
    }

    check(s"getOrUpdate1: $name") { (cache, metrics) =>
      for {
        value     <- cache.getOrUpdate1(0) { ("a", 0, none[IO[Unit]]).pure[IO] }
        _         <- IO { value shouldEqual "a".asLeft }

        value     <- cache.getOrUpdate1(0) { ("b", 1, none[IO[Unit]]).pure[IO] }
        _         <- IO { value shouldEqual 0.asRight.asRight }

        deferred0 <- Deferred[IO, Unit]
        deferred1 <- Deferred[IO, (String, Int, Option[IO[Unit]])]
        fiber     <- cache.getOrUpdate1(1) { deferred0.complete(()) *> deferred1.get }.start
        _         <- deferred0.get
        value     <- cache.getOrUpdate1(1) { ("d", 1, none[IO[Unit]]).pure[IO] }
        _         <- IO { value should matchPattern { case Right(Left(_)) => } }
        released  <- Deferred[IO, Unit]
        _         <- deferred1.complete(("c", 0, released.complete(()).void.some))
        value     <- value.traverse {
          case Right(a) => a.pure[IO]
          case Left(a)  => a
        }
        _     <- IO { value shouldEqual 0.asRight }
        value <- fiber.joinWithNever
        _     <- IO { value shouldEqual "c".asLeft }
        _     <- cache.clear
        _     <- released.complete(())
        _     <- metrics.expect(
          metrics.expectedGet(hit = true)      -> 2,
          metrics.expectedGet(hit = false)     -> 2,
          metrics.expectedLife                 -> 2,
          metrics.expectedClear                -> 1,
          metrics.expectedLoad(success = true) -> 2,
        )
      } yield {}
    }

    check(s"getOrUpdateResource: $name") { (cache, metrics) =>
      for {
        deferred0 <- Deferred[IO, Unit]
        deferred1 <- Deferred[IO, (Int, IO[Unit])]
        resource   = Resource
          .make { deferred0.complete(()) *> deferred1.get } { case (_, release) => release }
          .map { case (a, _) => a }
        fiber0    <- cache.getOrUpdateResource(0) { resource }.start
        _         <- deferred0.get
        fiber1    <- cache.getOrUpdateResource(0)(1.pure[Resource[IO, *]]).startEnsure
        released  <- Deferred[IO, Unit]
        _         <- deferred1.complete((0, released.get))
        value     <- fiber0.joinWithNever
        _         <- IO { value shouldEqual 0 }
        value     <- fiber1.joinWithNever
        _         <- IO { value shouldEqual 0 }
        _         <- released.complete(())
        _         <- metrics.expect(
          metrics.expectedGet(hit = false)     -> 1,
          metrics.expectedGet(hit = true)      -> 1,
          metrics.expectedLoad(success = true) -> 1
        )
      } yield {}
    }

    check(s"getOrUpdateResourceOpt: $name") { (cache, metrics) =>
      for {
        deferred  <- Deferred[IO, Unit]
        resource   = Resource
          .release { deferred.complete(()).void }
          .as(none[Int])
        value     <- cache.getOrUpdateResourceOpt(0) { resource }
        _         <- IO { value shouldEqual none }
        _         <- deferred.get
        deferred0 <- Deferred[IO, Unit]
        deferred1 <- Deferred[IO, (Option[Int], IO[Unit])]
        resource   = Resource
          .make { deferred0.complete(()) *> deferred1.get } { case (_, release) => release }
          .map { case (a, _) => a }
        fiber0    <- cache.getOrUpdateResourceOpt(0) { resource }.start
        _         <- deferred0.get
        fiber1    <- cache.getOrUpdateResourceOpt(0)(1.some.pure[Resource[IO, *]]).startEnsure
        released  <- Deferred[IO, Unit]
        _         <- deferred1.complete((0.some, released.get))
        value     <- fiber0.joinWithNever
        _         <- IO { value shouldEqual 0.some }
        value     <- fiber1.joinWithNever
        _         <- IO { value shouldEqual 0.some }
        _         <- released.complete(())
        _         <- metrics.expect(
          metrics.expectedGet(hit = false)     -> 2,
          metrics.expectedLoad(success = true) -> 2,
          metrics.expectedGet(hit = true)      -> 1
        )
      } yield {}
    }

    check(s"getOrUpdateOpt1: $name") { (cache, metrics) =>
      for {
        deferred <- Deferred[IO, Option[(Int, Option[IO[Unit]])]]
        value0   <- cache.getOrUpdateOpt1Ensure(0) { deferred.get }
        _        <- deferred.complete(none)
        value    <- value0.joinWithNever
        _        <- IO { value shouldEqual none[Int] }
        value    <- cache.getOrUpdateOpt1(0)((1, 1, none[IO[Unit]]).some.pure[IO])
        _        <- IO { value shouldEqual 1.asLeft.some }
        _        <- metrics.expect(
          metrics.expectedGet(hit = false)     -> 2,
          metrics.expectedLoad(success = true) -> 2
        )
      } yield {}
    }

    test(s"getOrUpdate1 does not fail after cache is released: $name") {
      val result = for {
        (cache, metrics) <- cacheAndMetrics.use { _.pure[IO] }
        a                <- cache.getOrUpdate1(0)((1, 1, none[IO[Unit]]).pure[IO])
        _                <- IO { a shouldEqual 1.asLeft }
        _                <- metrics.expect(
          metrics.expectedGet(hit = false)     -> 1,
          metrics.expectedLoad(success = true) -> 1,
        )
      } yield {}
      result.run()
    }

    test(s"getOrUpdate1 with release fails after cache is released: $name") {
      val result = for {
        (cache, metrics) <- cacheAndMetrics.use { _.pure[IO] }
        a                <- cache.getOrUpdate1(0)((1, 1, IO.unit.some).pure[IO]).attempt
        _                <- IO { a shouldEqual CacheReleasedError.asLeft }
        _                <- metrics.expect(
          metrics.expectedGet(hit = false)      -> 1,
          metrics.expectedLoad(success = false) -> 1
        )
      } yield {}
      result.run()
    }

    test(s"getOrUpdateOpt1 does not fail after cache is released: $name") {
      val result = for {
        (cache, metrics) <- cacheAndMetrics.use { _.pure[IO] }
        a                <- cache.getOrUpdateOpt1(0)((1, 1, none[IO[Unit]]).some.pure[IO])
        _                <- IO { a shouldEqual 1.asLeft.some }
        _                <- metrics.expect(
          metrics.expectedGet(hit = false)     -> 1,
          metrics.expectedLoad(success = true) -> 1,
        )
      } yield {}
      result.run()
    }

    test(s"getOrUpdateOpt1 with release fails after cache is released: $name") {
      val result = for {
        (cache, metrics) <- cacheAndMetrics.use { _.pure[IO] }
        a                <- cache.getOrUpdateOpt1(0)((1, 1, IO.unit.some).some.pure[IO]).attempt
        _                <- IO { a shouldEqual CacheReleasedError.asLeft }
        _                <- metrics.expect(
          metrics.expectedGet(hit = false)      -> 1,
          metrics.expectedLoad(success = false) -> 1,
        )
      } yield {}
      result.run()
    }

    check(s"cancel getOrUpdate1: $name") { (cache, metrics) =>
      for {
        deferred0 <- Deferred[IO, (Int, Option[IO[Unit]])]
        fiber0    <- cache.getOrUpdate1Ensure(0) { deferred0.get }
        fiber1    <- cache.getOrUpdate2(0) { IO.never }.startEnsure
        release   <- Deferred[IO, Unit]
        _         <- fiber0.cancel.start
        _         <- deferred0.complete((0, release.complete(()).void.some))
        value     <- fiber0.join
        _         <- IO { value shouldEqual Outcome.canceled }
        value     <- fiber1.joinWithNever
        _         <- IO { value shouldEqual 0.asRight }
        _         <- cache.remove(0)
        _         <- release.get
        _         <- metrics.expect(
          metrics.expectedGet(hit = false)     -> 1,
          metrics.expectedGet(hit = true)      -> 1,
          metrics.expectedLoad(success = true) -> 1,
          metrics.expectedLife                 -> 1
        )
      } yield {}
    }

    check(s"put while getOrUpdate: $name") { (cache, metrics) =>
      for {
        deferred <- Deferred[IO, Int]
        fiber    <- cache.getOrUpdateEnsure(0) { deferred.get }
        value    <- cache.put(0, 1)
        _        <- deferred.complete(0)
        value    <- value
        _        <- IO { value shouldEqual none }
        value    <- fiber.joinWithNever
        _        <- IO { value shouldEqual 1.asRight }
        value    <- cache.get(0)
        _        <- IO { value shouldEqual 1.some }
        _        <- metrics.expect(
          metrics.expectedGet(hit = true)      -> 2,
          metrics.expectedLoad(success = true) -> 1,
          metrics.expectedGet(hit = false)     -> 1,
          metrics.expectedPut                  -> 1,
          metrics.expectedLife                 -> 1
        )
      } yield {}
    }

    check(s"put while getOrUpdate1: $name") { (cache, metrics) =>
      for {
        deferred <- Deferred[IO, (Int, Option[IO[Unit]])]
        fiber    <- cache.getOrUpdate1Ensure(0) { deferred.get }
        value    <- cache.put(0, 1)
        release  <- Deferred[IO, Unit]
        _        <- deferred.complete((0, release.complete(()).void.some))
        _        <- release.get
        value    <- value
        _        <- IO { value shouldEqual none }
        value    <- fiber.joinWithNever
        _        <- IO { value shouldEqual 1.asRight }
        value    <- cache.get(0)
        _        <- IO { value shouldEqual 1.some }
        _        <- metrics.expect(
          metrics.expectedGet(hit = true)      -> 2,
          metrics.expectedLoad(success = true) -> 1,
          metrics.expectedGet(hit = false)     -> 1,
          metrics.expectedPut                  -> 1,
          metrics.expectedLife                 -> 1
        )
      } yield {}
    }

    check(s"put while getOrUpdate never: $name") { (cache, metrics) =>
      for {
        fiber <- cache.getOrUpdateEnsure(0) { IO.never[Int] }
        value <- cache.put(0, 0)
        value <- value
        _     <- IO { value shouldEqual none }
        value <- fiber.joinWithNever
        _     <- IO { value shouldEqual 0.asRight }
        value <- cache.get(0)
        _     <- IO { value shouldEqual 0.some }
        _     <- metrics.expect(
          metrics.expectedGet(hit = false) -> 1,
          metrics.expectedPut              -> 1,
          metrics.expectedGet(hit = true)  -> 2
        )
      } yield {}
    }

    check(s"put while getOrUpdate1 never: $name") { (cache, metrics) =>
      for {
        fiber <- cache.getOrUpdate1Ensure(0) { IO.never[(Int, Option[IO[Unit]])] }
        value <- cache.put(0, 0)
        value <- value
        _     <- IO { value shouldEqual none }
        value <- fiber.joinWithNever
        _     <- IO { value shouldEqual 0.asRight }
        value <- cache.get(0)
        _     <- IO { value shouldEqual 0.some }
        _     <- metrics.expect(
          metrics.expectedGet(hit = false) -> 1,
          metrics.expectedPut              -> 1,
          metrics.expectedGet(hit = true)  -> 2
        )
      } yield {}
    }

    check(s"put while getOrUpdate failed: $name") { (cache, metrics) =>
      for {
        deferred <- Deferred[IO, IO[Int]]
        fiber    <- cache.getOrUpdateEnsure(0) { deferred.get.flatten }
        value    <- cache.put(0, 1)
        _        <- deferred.complete(TestError.raiseError[IO, Int])
        value    <- value
        _        <- IO { value shouldEqual none }
        value    <- fiber.joinWithNever
        _        <- IO { value shouldEqual 1.asRight }
        value    <- cache.get(0)
        _        <- IO { value shouldEqual 1.some }
        _        <- metrics.expect(
          metrics.expectedGet(hit = false)      -> 1,
          metrics.expectedPut                   -> 1,
          metrics.expectedLoad(success = false) -> 1,
          metrics.expectedGet(hit = true)       -> 2
        )
      } yield {}
    }

    check(s"put while getOrUpdate1 failed: $name") { (cache, metrics) =>
      for {
        deferred <- Deferred[IO, IO[(Int, Option[IO[Unit]])]]
        fiber    <- cache.getOrUpdate1Ensure(0) { deferred.get.flatten }
        value    <- cache.put(0, 1)
        _        <- deferred.complete(TestError.raiseError[IO, (Int, Option[IO[Unit]])])
        value    <- value
        _        <- IO { value shouldEqual none }
        value    <- fiber.joinWithNever
        _        <- IO { value shouldEqual 1.asRight }
        value    <- cache.get(0)
        _        <- IO { value shouldEqual 1.some }
        _        <- metrics.expect(
          metrics.expectedGet(hit = false)      -> 1,
          metrics.expectedPut                   -> 1,
          metrics.expectedLoad(success = false) -> 1,
          metrics.expectedGet(hit = true)       -> 2
        )
      } yield {}
    }

    check(s"get while getOrUpdate: $name") { (cache, metrics) =>
      for {
        deferred <- Deferred[IO, Int]
        value0   <- cache.getOrUpdateEnsure(0) { deferred.get }
        value1   <- cache.get(0).startEnsure
        _        <- deferred.complete(0)
        value    <- value0.joinWithNever
        _        <- IO { value shouldEqual 0.asRight }
        value    <- value1.joinWithNever
        _        <- IO { value shouldEqual 0.some }
        _        <- metrics.expect(
          metrics.expectedGet(hit = false)     -> 1,
          metrics.expectedLoad(success = true) -> 1,
          metrics.expectedGet(hit = true)      -> 1
        )
      } yield {}
    }

    check(s"get while getOrUpdate1: $name") { (cache, metrics) =>
      for {
        deferred <- Deferred[IO, (Int, Option[IO[Unit]])]
        released <- Deferred[IO, Unit]
        value0   <- cache.getOrUpdate1Ensure(0) { deferred.get }
        value1   <- cache.get(0).startEnsure
        _        <- deferred.complete((0, released.get.some))
        value    <- value0.joinWithNever
        _        <- IO { value shouldEqual 0.asRight }
        value    <- value1.joinWithNever
        _        <- IO { value shouldEqual 0.some }
        _        <- released.complete(())
        _        <- metrics.expect(
          metrics.expectedGet(hit = false)     -> 1,
          metrics.expectedLoad(success = true) -> 1,
          metrics.expectedGet(hit = true)      -> 1
        )
      } yield {}
    }

    check(s"get while getOrUpdate failed: $name") { (cache, metrics) =>
      for {
        deferred <- Deferred[IO, IO[Int]]
        value0   <- cache.getOrUpdateEnsure(0) { deferred.get.flatten }
        value1   <- cache.get(0).startEnsure
        _        <- deferred.complete(TestError.raiseError[IO, Int])
        value    <- value0.joinWithNever
        _        <- IO { value shouldEqual TestError.asLeft }
        value    <- value1.joinWithNever
        _        <- IO { value shouldEqual none[Int] }
        _        <- metrics.expect(
          metrics.expectedGet(hit = false)      -> 2,
          metrics.expectedLoad(success = false) -> 1
        )
      } yield {}
    }

    check(s"get while getOrUpdate1 failed: $name") { (cache, metrics) =>
      for {
        deferred <- Deferred[IO, IO[(Int, Option[IO[Unit]])]]
        value0   <- cache.getOrUpdate1Ensure(0) { deferred.get.flatten }
        value1   <- cache.get(0).startEnsure
        _        <- deferred.complete(TestError.raiseError[IO, (Int, Option[IO[Unit]])])
        value    <- value0.joinWithNever
        _        <- IO { value shouldEqual TestError.asLeft }
        value    <- value1.joinWithNever
        _        <- IO { value shouldEqual none[Int] }
        _        <- metrics.expect(
          metrics.expectedGet(hit = false)      -> 2,
          metrics.expectedLoad(success = false) -> 1
        )
      } yield {}
    }

    check(s"remove while getOrUpdate: $name") { (cache, metrics) =>
      for {
        deferred <- Deferred[IO, Int]
        value0   <- cache.getOrUpdateEnsure(0) { deferred.get }
        value1   <- cache.remove(0)
        _        <- deferred.complete(0)
        value    <- value0.joinWithNever
        _        <- IO { value shouldEqual 0.asRight }
        value    <- value1
        _        <- IO { value shouldEqual None }
        _        <- metrics.expect(
          metrics.expectedGet(hit = false)     -> 1,
          metrics.expectedLoad(success = true) -> 1,
          metrics.expectedLife                 -> 1
        )
      } yield {}
    }

    check(s"remove while getOrUpdate1: $name") { (cache, metrics) =>
      for {
        deferred <- Deferred[IO, (Int, Option[IO[Unit]])]
        fiber    <- cache.getOrUpdate1Ensure(0) { deferred.get }
        value1   <- cache.remove(0)
        release  <- Deferred[IO, Unit]
        released <- Deferred[IO, Boolean]
        _        <- deferred.complete((0, (release.get *> released.complete(true).void).some))
        value    <- fiber.joinWithNever
        _        <- IO { value shouldEqual 0.asRight }
        value    <- value1.startEnsure
        _        <- release.complete(())
        value    <- value.joinWithNever
        released <- released.get
        _        <- IO { released shouldEqual true }
        _        <- IO { value shouldEqual None }
        _        <- metrics.expect(
          metrics.expectedGet(hit = false)     -> 1,
          metrics.expectedLoad(success = true) -> 1,
          metrics.expectedLife                 -> 1
        )
      } yield {}
    }

    check(s"remove while getOrUpdate failed: $name") { (cache, metrics) =>
      for {
        deferred <- Deferred[IO, IO[Int]]
        fiber    <- cache.getOrUpdateEnsure(0) { deferred.get.flatten }
        value    <- cache.remove(0)
        _        <- deferred.complete(TestError.raiseError[IO, Int])
        value    <- value
        _        <- IO { value shouldEqual none }
        value    <- fiber.joinWithNever
        _        <- IO { value shouldEqual TestError.asLeft }
        value    <- cache.get(0)
        _        <- IO { value shouldEqual none }
        _        <- metrics.expect(
          metrics.expectedGet(hit = false)      -> 2,
          metrics.expectedLoad(success = false) -> 1
        )
      } yield {}
    }

    check(s"remove while getOrUpdate1 failed: $name") { (cache, metrics) =>
      for {
        deferred <- Deferred[IO, IO[(Int, Option[IO[Unit]])]]
        fiber    <- cache.getOrUpdate1Ensure(0) { deferred.get.flatten }
        value    <- cache.remove(0)
        _        <- deferred.complete(TestError.raiseError[IO, (Int, Option[IO[Unit]])])
        value    <- value
        _        <- IO { value shouldEqual none }
        value    <- fiber.joinWithNever
        _        <- IO { value shouldEqual TestError.asLeft }
        value    <- cache.get(0)
        _        <- IO { value shouldEqual none }
        _        <- metrics.expect(
          metrics.expectedGet(hit = false)      -> 2,
          metrics.expectedLoad(success = false) -> 1
        )
      } yield {}
    }

    check(s"clear while getOrUpdate: $name") { (cache, metrics) =>
      for {
        deferred <- Deferred[IO, Int]
        value    <- cache.getOrUpdateEnsure(0) { deferred.get }
        keys     <- cache.keys
        _        <- IO { keys shouldEqual Set(0) }
        _        <- cache.clear
        keys     <- cache.keys
        _        <- IO { keys shouldEqual Set.empty }
        _        <- deferred.complete(0)
        value    <- value.joinWithNever
        _        <- IO { value shouldEqual 0.asRight }
        keys     <- cache.keys
        _        <- IO { keys shouldEqual Set.empty }
        _        <- metrics.expect(
          metrics.expectedKeys                 -> 3,
          metrics.expectedGet(hit = false)     -> 1,
          metrics.expectedLife                 -> 1,
          metrics.expectedClear                -> 1,
          metrics.expectedLoad(success = true) -> 1
        )
      } yield {}
    }

    check(s"clear while getOrUpdate1: $name") { (cache, metrics) =>
      for {
        release  <- Deferred[IO, Unit]
        released <- Ref[IO].of(false)
        value    <- cache.getOrUpdate1(0) { (0, 0, (release.get *> released.set(true)).some).pure[IO] }
        _        <- IO { value shouldEqual 0.asLeft }
        keys     <- cache.keys
        _        <- IO { keys shouldEqual Set(0) }
        clear    <- cache.clear
        keys     <- cache.keys
        _        <- IO { keys shouldEqual Set.empty }
        _        <- release.complete(())
        _        <- clear
        released <- released.get
        _        <- IO { released shouldEqual true }
        keys     <- cache.keys
        _        <- IO { keys shouldEqual Set.empty }
        _ <- metrics.expect(
          metrics.expectedKeys                 -> 3,
          metrics.expectedGet(hit = false)     -> 1,
          metrics.expectedLife                 -> 1,
          metrics.expectedClear                -> 1,
          metrics.expectedLoad(success = true) -> 1
        )
      } yield {}
    }

    check(s"clear while getOrUpdate1 loading: $name") { (cache, metrics) =>
      for {
        deferred <- Deferred[IO, (Int, Option[IO[Unit]])]
        value    <- cache.getOrUpdate1Ensure(0) { deferred.get }
        keys     <- cache.keys
        _        <- IO { keys shouldEqual Set(0) }
        clear    <- cache.clear
        keys     <- cache.keys
        _        <- IO { keys shouldEqual Set.empty }
        release  <- Deferred[IO, Unit]
        released <- Ref[IO].of(false)
        _        <- deferred.complete((0, (release.get *> released.set(true)).some))
        value    <- value.joinWithNever
        _        <- IO { value shouldEqual 0.asRight }
        keys     <- cache.keys
        _        <- IO { keys shouldEqual Set.empty }
        _        <- release.complete(())
        _        <- clear
        released <- released.get
        _        <- IO { released shouldEqual true }
        _ <- metrics.expect(
          metrics.expectedKeys                 -> 3,
          metrics.expectedGet(hit = false)     -> 1,
          metrics.expectedLife                 -> 1,
          metrics.expectedClear                -> 1,
          metrics.expectedLoad(success = true) -> 1
        )
      } yield {}
    }

    check(s"keys: $name") { (cache, metrics) =>
      for {
        _    <- cache.put(0, 0)
        keys <- cache.keys
        _    <- IO { keys shouldEqual Set(0) }
        _    <- cache.put(1, 1)
        keys <- cache.keys
        _    <- IO { keys shouldEqual Set(0, 1) }
        _    <- cache.put(2, 2)
        keys <- cache.keys
        _    <- IO { keys shouldEqual Set(0, 1, 2) }
        _    <- cache.clear
        keys <- cache.keys
        _    <- IO { keys shouldEqual Set.empty }
        _    <- metrics.expect(
          metrics.expectedPut   -> 3,
          metrics.expectedKeys  -> 4,
          metrics.expectedClear -> 1,
          metrics.expectedLife  -> 3
        )
      } yield {}
    }

    check(s"values: $name") { (cache, metrics) =>
      for {
        _      <- cache.put(0, 0)
        values <- cache.valuesFlatten
        _      <- IO { values shouldEqual Map((0, 0)) }
        _      <- cache.put(1, 1)
        values <- cache.valuesFlatten
        _      <- IO { values shouldEqual Map((0, 0), (1, 1)) }
        _      <- cache.put(2, 2)
        values <- cache.valuesFlatten
        _      <- IO { values shouldEqual Map((0, 0), (1, 1), (2, 2)) }
        _      <- cache.clear
        values <- cache.valuesFlatten
        _      <- IO { values shouldEqual Map.empty }
        _      <- metrics.expect(
          metrics.expectedPut    -> 3,
          metrics.expectedValues -> 4,
          metrics.expectedClear  -> 1,
          metrics.expectedLife   -> 3
        )
      } yield {}
    }

    check(s"values1: $name") { (cache, metrics) =>
      for {
        _ <- cache.put(0, 0)
        a <- cache.values1
        _ <- IO { a shouldEqual Map((0, 0.asRight)) }
        _ <- cache.put(1, 1)
        a <- cache.values1
        _ <- IO { a shouldEqual Map((0, 0.asRight), (1, 1.asRight)) }
        _ <- cache.put(2, 2)
        d <- Deferred[IO, Int]
        b <- cache.getOrUpdateEnsure(3) { d.get }
        a <- cache.values1
        _ <- IO { a.map { case (k, v) => (k, v.toOption) } shouldEqual Map((0, 0.some), (1, 1.some), (2, 2.some), (3, none)) }
        _ <- d.complete(3)
        _ <- b.join
        a <- cache.values1
        _ <- IO { a shouldEqual Map((0, 0.asRight), (1, 1.asRight), (2, 2.asRight), (3, 3.asRight)) }
        _ <- cache.clear
        a <- cache.values1
        _ <- IO { a shouldEqual Map.empty }
        _ <- metrics.expect(
          metrics.expectedGet(hit = false)     -> 1,
          metrics.expectedPut                  -> 3,
          metrics.expectedClear                -> 1,
          metrics.expectedLoad(success = true) -> 1,
          metrics.expectedValues               -> 5,
          metrics.expectedLife                 -> 4
        )
      } yield {}
    }

    check(s"cancellation: $name") { (cache, metrics) =>
      for {
        deferred      <- Deferred[IO, Int]
        fiber         <- cache.getOrUpdateEnsure(0) { deferred.get }
        _             <- fiber.cancel.start
        _             <- deferred.complete(0)
        cancelOutcome <- fiber.join
        _             <- IO { cancelOutcome shouldEqual Outcome.canceled }
        value         <- cache.get(0)
        _             <- IO { value shouldEqual 0.some }
        _             <- metrics.expect(
          metrics.expectedGet(hit = false)     -> 1,
          metrics.expectedLoad(success = true) -> 1,
          metrics.expectedGet(hit = true)      -> 1
        )
      } yield {}
    }

    ignore(s"cancellation proper: $name") {
      cacheAndMetrics
        .use { case (cache, metrics) =>
          for {
            deferred <- Deferred[IO, Int]
            fiber    <- cache.getOrUpdateEnsure(0)(deferred.get)
            fiber    <- fiber.cancel.start
            result   <- cache.get(0)
            _        <- IO { result shouldEqual none }
            _        <- deferred.complete(0)
            _        <- fiber.joinWithNever
            _        <- metrics.expect(metrics.expectedGet(hit = false) -> 2)
          } yield {}
        }
        .run()
    }

    check(s"no leak in case of failure: $name") { (cache, metrics) =>
      for {
        result <- cache.getOrUpdate(0)(TestError.raiseError[IO, Int]).attempt
        _      <- IO { result shouldEqual TestError.asLeft[Int] }
        result <- cache.getOrUpdate(0)(0.pure[IO]).attempt
        _      <- IO { result shouldEqual 0.asRight }
        result <- cache.getOrUpdate(0)(TestError.raiseError[IO, Int]).attempt
        _      <- IO { result shouldEqual 0.asRight }
        _      <- metrics.expect(
          metrics.expectedGet(hit = false)      -> 2,
          metrics.expectedLoad(success = false) -> 1,
          metrics.expectedLoad(success = true)  -> 1,
          metrics.expectedGet(hit = true)       -> 1
        )
      } yield {}
    }

    check(s"foldMap: $name") { (cache, metrics) =>
      for {
        _ <- cache.put(0, 1)
        expect = (a: Int, i: Int) => for {
          b <- cache.foldMap { case (_, b) => b.map { _.pure[IO] }.merge }
          _ <- IO { b shouldEqual a }
          _ <- metrics.expect(
            metrics.expectedPut     -> i,
            metrics.expectedFoldMap -> i,
          )
        } yield {}
        _ <- expect(1, 1)
        _ <- cache.put(1, 2)
        _ <- expect(3, 2)
      } yield {}
    }

    check(s"foldMapPar: $name") { (cache, metrics) =>
      for {
        d0 <- Deferred[IO, Unit]
        d1 <- Deferred[IO, Int]
        d2 <- Deferred[IO, Unit]
        d3 <- Deferred[IO, Int]
        d4 <- Deferred[IO, Unit]
        d5 <- Deferred[IO, Int]
        _ <- cache.getOrUpdateEnsure(0) { d0.complete(()).productR(d1.get) }
        _ <- cache.getOrUpdateEnsure(1) { d2.complete(()).productR(d3.get) }
        _ <- cache.getOrUpdateEnsure(2) { d4.complete(()).productR(d5.get) }
        fiber <- cache
          .foldMapPar { case (_, b) => b.map { _.pure[IO] }.merge }
          .start
        _ <- d0.get
        _ <- d2.get
        _ <- d4.get
        _ <- d1.complete(1)
        _ <- d3.complete(2)
        _ <- d5.complete(3)
        a <- fiber.join
        _ <- IO { a shouldEqual 6.pure[Outcome[IO, Throwable, *]] }
        _ <- metrics.expect(
          metrics.expectedGet(hit = false)     -> 3,
          metrics.expectedLoad(success = true) -> 3,
          metrics.expectedFoldMap              -> 1
        )
      } yield {}
    }

    check(s"each release performed exactly once during `getOrUpdate1` and `put` race: $name") { (cache, _) =>
      for {
        resultRef1 <- Ref[IO].of(0)
        resultRef2 <- Ref[IO].of(0)
        n = 100000
        range = (1 to n).toList

        // For `getOrUpdate*` we don't know how many times the resource will be run,
        // so we use increment/decrement as a way to check that the resource is released exactly once.
        valueResource = (i: Int) => Resource.make(resultRef1.update(_ + i).as(i))(_ => resultRef1.update(_ - i))
        f1 <- range.parTraverse { i => cache.getOrUpdateResource(0)(valueResource(i)) }.start

        // For `put` we know that the resource will be written and released every time,
        // so we increment on release and check that the final value is equal to the sum of the range.
        f2 <- range.parTraverse(i => cache.put(0, 0, resultRef2.update(_ + i))).start

        expectedResult = range.sum

        _ <- f1.joinWithNever.void
        _ <- f2.joinWithNever.flatMap(_.sequence)
        _ <- cache.clear.flatten

        result1 <- resultRef1.get
        result2 <- resultRef2.get
        _ <- IO { result1 shouldEqual 0 }
        _ <- IO { result2 shouldEqual expectedResult }
      } yield {}
    }

    check(s"each release performed exactly once during `put` and `remove` race: $name") { (cache, _) =>
      for {
        resultRef <- Ref[IO].of(0)
        n = 100000
        range = (1 to n).toList

        f1 <- range.parTraverse(i => cache.put(0, 0, resultRef.update(_ + i))).start
        f2 <- cache.remove(0).replicateA(n).start

        expectedResult = range.sum

        _ <- f1.joinWithNever.flatMap(_.sequence)
        _ <- f2.joinWithNever.flatMap(_.sequence)
        _ <- cache.clear.flatten

        result <- resultRef.get
        _ <- IO { result shouldEqual expectedResult }
      } yield {}
    }

    check(s"each release performed exactly once during `getOrUpdate1`, `put` and `remove` race: $name") { (cache, _) =>
      for {
        resultRef1 <- Ref[IO].of(0)
        resultRef2 <- Ref[IO].of(0)
        n = 100000
        range = (1 to n).toList

        // For `getOrUpdate*` we don't know how many times the resource will be run,
        // so we use increment/decrement as a way to check that the resource is released exactly once.
        valueResource = (i: Int) => Resource.make(resultRef1.update(_ + i).as(i))(_ => resultRef1.update(_ - i))
        f1 <- range.parTraverse { i => cache.getOrUpdateResource(0)(valueResource(i)) }.start

        // For `put` we know that the resource will be written and released every time,
        // so we increment on release and check that the final value is equal to the sum of the range.
        f2 <- range.parTraverse(i => cache.put(0, 0, resultRef2.update(_ + i))).start

        f3 <- cache.remove(0).replicateA(n).start

        expectedResult = range.sum

        _ <- f1.joinWithNever.void
        _ <- f2.joinWithNever.flatMap(_.sequence)
        _ <- f3.joinWithNever.flatMap(_.sequence)
        _ <- cache.clear.flatten

        result1 <- resultRef1.get
        result2 <- resultRef2.get
        _ <- IO { result1 shouldEqual 0 }
        _ <- IO { result2 shouldEqual expectedResult }
      } yield {}
    }

    check(s"failing loads don't interfere with releases during `getOrUpdate1`, `put` and `remove` race: $name") { (cache, _) =>
      for {
        resultRef1 <- Ref[IO].of(0)
        resultRef2 <- Ref[IO].of(0)
        resultRef3 <- Ref[IO].of(0)
        n = 100000
        range = (1 to n).toList

        // For `getOrUpdate*` we don't know how many times the resource will be run,
        // so we use increment/decrement as a way to check that the resource is released exactly once.
        valueResource = (i: Int) => Resource.make(resultRef1.update(_ + i).as(i))(_ => resultRef1.update(_ - i))
        f1 <- range.parTraverse { i =>
          cache.getOrUpdateResource(0)(valueResource(i)).recover { case _ => -1 }
        }.start

        failingResource = (i: Int) =>
          Resource.make(new Exception("Boom").raiseError[IO, Int])(_ => resultRef2.update(_ - i))
        f2 <- range.parTraverse { i =>
          cache.getOrUpdateResource(0)(failingResource(i)).recover { case _ => -1 }
        }.start

        // For `put` we know that the resource will be written and released every time,
        // so we increment on release and check that the final value is equal to the sum of the range.
        f3 <- range.parTraverse(i => cache.put(0, 0, resultRef3.update(_ + i))).start

        f4 <- cache.remove(0).parReplicateA(n).start

        expectedResult = range.sum

        _ <- f1.joinWithNever.void
        _ <- f2.joinWithNever.void
        _ <- f3.joinWithNever.flatMap(_.sequence)
        _ <- f4.joinWithNever.flatMap(_.sequence)
        _ <- cache.clear.flatten

        result1 <- resultRef1.get
        result2 <- resultRef2.get
        result3 <- resultRef3.get
        _ <- IO { result1 shouldEqual 0 }
        _ <- IO { result2 shouldEqual 0 }
        _ <- IO { result3 shouldEqual expectedResult }
      } yield ()
    }
  }
}

object CacheSpec {

  class CacheMetricsProbe(interactions: Ref[IO, Map[String, Int]]) extends CacheMetrics[IO] with Matchers with Eventually {
    private def inc(op: String) = interactions.update(i => i.updated(op, i.getOrElse(op, 0) + 1))

    // results of metrics.life calls might take some time
    def expect(calls: (String, Int)*): IO[Assertion] = eventually(timeout(200.millis), interval(50.millis)) {
      interactions
        .get
        .flatMap(is => IO { is shouldBe calls.toMap })
    }

    def expectedGet(hit: Boolean): String = s"get(hit=$hit)"
    def expectedLoad(success: Boolean): String = s"load(time=..., success=$success)"
    val expectedLife: String = "life(time=...)"
    val expectedPut: String = "put"
    def expectedSize(size: Int): String = s"size(size=$size)"
    val expectedSize: String = "size(latency=...)"
    val expectedValues: String = "values(latency=...)"
    val expectedKeys: String = "keys(latency=...)"
    val expectedClear: String = "clear(latency=...)"
    val expectedFoldMap: String = "foldMap(latency=...)"

    def get(hit: Boolean): IO[Unit] = inc(expectedGet(hit))
    def load(time: FiniteDuration, success: Boolean): IO[Unit] = inc(expectedLoad(success))
    def life(time: FiniteDuration): IO[Unit] = inc(expectedLife)
    def put: IO[Unit] = inc(expectedPut)
    def size(size: Int): IO[Unit] = inc(expectedSize(size))
    def size(latency: FiniteDuration): IO[Unit] = inc(expectedSize)
    def values(latency: FiniteDuration): IO[Unit] = inc(expectedValues)
    def keys(latency: FiniteDuration): IO[Unit] = inc(expectedKeys)
    def clear(latency: FiniteDuration): IO[Unit] = inc(expectedClear)
    def foldMap(latency: FiniteDuration): IO[Unit] = inc(expectedFoldMap)
  }
  object CacheMetricsProbe {
    def of: IO[CacheMetricsProbe] = Ref.of[IO, Map[String, Int]](Map.empty).map(new CacheMetricsProbe(_))
  }

  case object TestError extends RuntimeException with NoStackTrace

  implicit class CacheSpecCacheOps[F[_], K, V](val self: Cache[F, K, V]) extends AnyVal {

    def valuesFlatten(implicit F: Monad[F]): F[Map[K, V]] = {
      for {
        values <- self.values
        zero    = Map.empty[K, V].pure[F]
        values <- values.foldLeft(zero) { case (map, (key, value)) =>
          for {
            map <- map
            value <- value
          } yield {
            map.updated(key, value)
          }
        }
      } yield values
    }


    def getOrUpdateEnsure(key: K)(value: => F[V])(implicit F: Concurrent[F]): F[Fiber[F, Throwable, Either[Throwable, V]]] = {
      for {
        deferred <- Deferred[F, Unit]
        fiber    <- self
          .getOrUpdate(key) {
            for {
              _ <- deferred.complete(())
              a <- value
            } yield a
          }
          .attempt
          .start
        _        <- deferred.get
      } yield fiber
    }

    def getOrUpdateOptEnsure(key: K)(value: => F[Option[V]])(implicit F: Concurrent[F]): F[Fiber[F, Throwable, Either[Throwable, Option[V]]]] = {
      for {
        deferred <- Deferred[F, Unit]
        fiber    <- self
          .getOrUpdateOpt(key) {
            deferred
              .complete(())
              .productR { value }
          }
          .attempt
          .start
        _        <- deferred.get
      } yield fiber
    }

    def getOrUpdate1Ensure(
      key: K)(
      value: => F[(V, Option[Cache[F, K, V]#Release])])(implicit
      F: Concurrent[F]
    ): F[Fiber[F, Throwable, Either[Throwable, V]]] = {
      for {
        deferred <- Deferred[F, Unit]
        fiber    <- self
          .getOrUpdate1(key) {
            deferred
              .complete(())
              .productR { value }
              .map { case (value, release) => (value, value, release) }
          }
          .flatMap {
            case Right(Right(a)) => a.pure[F]
            case Right(Left(a))  => a
            case Left(a)         => a.pure[F]
          }
          .attempt
          .start
        _        <- deferred.get
      } yield fiber
    }

    def getOrUpdateOpt1Ensure(
      key: K)(
      value: => F[Option[(V, Option[Cache[F, K, V]#Release])]])(implicit
      F: Concurrent[F]
    ): F[Fiber[F, Throwable, Option[V]]] = {
      for {
        deferred <- Deferred[F, Unit]
        fiber    <- self
          .getOrUpdateOpt1[V](key) {
            deferred
              .complete(())
              .productR { value }
              .map { _.map { case (value, release) => (value, value, release) } }
          }
          .flatMap {
            case Some(Right(Right(a))) => a.some.pure[F]
            case Some(Right(Left(a)))  => a.map { _.some }
            case Some(Left(a))         => a.some.pure[F]
            case None                  => none[V].pure[F]
          }
          .start
        _        <- deferred.get
      } yield fiber
    }
  }
}
