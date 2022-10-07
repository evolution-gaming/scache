package com.evolutiongaming.scache

import cats.Monad
import cats.arrow.FunctionK
import cats.effect.implicits._
import cats.effect._
import cats.syntax.all._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.scache.IOSuite._
import com.evolutiongaming.smetrics.CollectorRegistry
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class CacheSpec extends AsyncFunSuite with Matchers {
  import CacheSpec._

  private val expiringCache = Cache.expiring[IO, Int, Int](
    config = ExpiringCache.Config[IO, Int, Int](expireAfterRead = 1.minute),
    partitions = None,
  )

  for {
    (name, cache0) <- List(
      ("default"               , Cache.loading1[IO, Int, Int]),
      ("no partitions"         , LoadingCache.of(LoadingCache.EntryRefs.empty[IO, Int, Int])),
      ("expiring"              , expiringCache),
      ("expiring no partitions", ExpiringCache.of[IO, Int, Int](ExpiringCache.Config(expireAfterRead = 1.minute))))
  } yield {

    val cache = for {
      cache   <- cache0
      metrics <- CacheMetrics.of(CollectorRegistry.empty[IO])
      cache   <- cache.withMetrics(metrics("name"))
      cache   <- cache
        .mapK(FunctionK.id, FunctionK.id)
        .withFence
    } yield {
      cache.mapK(FunctionK.id, FunctionK.id)
    }

    test(s"get: $name") {
      cache
        .use { cache =>
          for {
            value <- cache.get(0)
          } yield {
            value shouldEqual none[Int]
          }
        }
        .run()
    }

    test(s"get1: $name") {
      cache
        .use { cache =>
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
          } yield {}
        }
        .run()
    }

    test(s"get succeeds after cache is released: $name") {
      val result = for {
        cache <- cache.use { _.pure[IO] }
        a     <- cache.get(0)
        _      = a shouldEqual none
      } yield {}
      result.run()
    }


    test(s"getOrElse1: $name") {
      cache
        .use { cache =>
          for {
            value <- cache.getOrElse1(0, 1.pure[IO])
            _      = value shouldEqual 1
            _     <- cache.put(0, 2)
            value <- cache.getOrElse1(0, 1.pure[IO])
            _      = value shouldEqual 2
          } yield {}
        }
        .run()
    }


    test(s"getOrElse1 succeeds after cache is released: $name") {
      val result = for {
        cache <- cache.use { _.pure[IO] }
        a     <- cache.getOrElse1(0, 1.pure[IO])
        _      = a shouldEqual 1
      } yield {}
      result.run()
    }


    test(s"put: $name") {
      cache
        .use { cache =>
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
          } yield {}
        }
        .run()
    }


    test(s"put succeeds after cache is released: $name") {
      val result = for {
        cache <- cache.use { _.pure[IO] }
        a     <- cache.put(0, 0).flatten
        _      = a shouldEqual none
      } yield {}
      result.run()
    }


    test(s"put releasable: $name") {
      cache
        .use { cache =>
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
          } yield {}
        }
        .run()
    }


    test(s"put releasable fails after cache is released: $name") {
      val result = for {
        cache <- cache.use { _.pure[IO] }
        a     <- cache.put(0, 0, ().pure[IO]).flatten.attempt
        _      = a shouldEqual CacheReleasedError.asLeft
      } yield {}
      result.run()
    }


    test(s"contains: $name") {
      cache
          .use { cache =>
          for {
            a     <- cache.contains(0)
            _     <- IO { a shouldBe false }
            a     <- cache.contains(1)
            _     <- IO { a shouldBe false }

            _     <- cache.put(0, 0)

            a     <- cache.contains(0)
            _     <- IO { a shouldBe true }
            a     <- cache.contains(1)
            _     <- IO { a shouldBe false }

            _     <- cache.put(1, 1)

            a     <- cache.contains(0)
            _     <- IO { a shouldBe true }
            a     <- cache.contains(1)
            _     <- IO { a shouldBe true }

            _     <- cache.remove(0)

            a     <- cache.contains(0)
            _     <- IO { a shouldBe false }
            a     <- cache.contains(1)
            _     <- IO { a shouldBe true }

            _     <- cache.clear

            a     <- cache.contains(0)
            _     <- IO { a shouldBe false }
            a     <- cache.contains(1)
            _     <- IO { a shouldBe false }
          } yield {}
        }
        .run()
    }


    test(s"size: $name") {
      cache
        .use { cache =>
          for {
            size0 <- cache.size
            _     <- cache.put(0, 0)
            size1 <- cache.size
            _     <- cache.put(0, 1)
            size2 <- cache.size
            _     <- cache.put(1, 1)
            size3 <- cache.size
            _     <- cache.remove(0)
            size4 <- cache.size
            _     <- cache.clear
            size5 <- cache.size
          } yield {
            size0 shouldEqual 0
            size1 shouldEqual 1
            size2 shouldEqual 1
            size3 shouldEqual 2
            size4 shouldEqual 1
            size5 shouldEqual 0
          }
        }
        .run()
    }


    test(s"size succeeds after cache is released: $name") {
      val result = for {
        cache <- cache.use { cache => cache.put(0, 0).flatten as cache }
        a     <- cache.size
        _      = a shouldEqual 0
      } yield {}
      result.run()
    }


    test(s"remove: $name") {
      cache
        .use { cache =>
          for {
            _     <- cache.put(0, 0)
            value <- cache.remove(0)
            value <- value
            _     <- IO { value shouldEqual 0.some }
            value <- cache.get(0)
            _     <- IO { value shouldEqual none }
          } yield {}
        }
        .run()
    }


    test(s"remove succeeds after cache is released: $name") {
      val result = for {
        cache <- cache.use { _.pure[IO] }
        a     <- cache.remove(0).flatten
        _      = a shouldEqual none
      } yield {}
      result.run()
    }


    test(s"clear: $name") {
      cache
        .use { cache =>
          for {
            _      <- cache.put(0, 0)
            _      <- cache.put(1, 1)
            _      <- cache.clear
            value0 <- cache.get(0)
            value1 <- cache.get(1)
          } yield {
            value0 shouldEqual none[Int]
            value1 shouldEqual none[Int]
          }
        }
        .run()
    }


    test(s"clear succeeds after cache is released: $name") {
      val result = for {
        cache <- cache.use { _.pure[IO] }
        _     <- cache.clear.flatten
      } yield {}
      result.run()
    }


    test(s"clear releasable: $name") {
      cache
        .use { cache =>
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
            keys      <- cache.keys
            _         <- IO { keys shouldEqual Set.empty }
            _         <- release.complete(()).start
            _         <- clear
            value     <- released0.get
            _         <- IO { value shouldEqual true }
            value     <- released1.get
            _         <- IO { value shouldEqual true }
          } yield {}
        }
        .run()
    }


    test(s"put & get many: $name") {

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

      cache
        .use { cache =>
          for {
            result0 <- getAll(cache)
            _       <- values.foldMapM { key => cache.put(key, key).void }
            result1 <- getAll(cache)
          } yield {
            result0.flatten.reverse shouldEqual List.empty
            result1.flatten.reverse shouldEqual values
          }
        }
        .run()
    }


    test(s"getOrUpdate: $name") {
      cache
        .use { cache =>
          for {
            deferred <- Deferred[IO, Int]
            value0   <- cache.getOrUpdateEnsure(0) { deferred.get }
            value2   <- cache.getOrUpdate(0)(1.pure[IO]).startEnsure
            _        <- deferred.complete(0)
            value0   <- value0.joinWithNever
            value1   <- value2.joinWithNever
            _        <- IO { value0 shouldEqual 0.asRight }
            _        <- IO { value1 shouldEqual 0 }
          } yield {}
        }
        .run()
    }


    test(s"getOrUpdate succeeds after cache is released: $name") {
      val result = for {
        cache <- cache.use { _.pure[IO] }
        a     <- cache.getOrUpdate(0)(1.pure[IO])
        _      = a shouldEqual 1
      } yield {}
      result.run()
    }


    test(s"getOrUpdateOpt: $name") {
      cache
        .use { cache =>
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
          } yield {}
        }
        .run()
    }


    test(s"getOrUpdate1: $name") {
      cache
        .use { cache =>
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
            _         <- IO { value shouldEqual 0.asRight }
            value     <- fiber.joinWithNever
            _         <- IO { value shouldEqual "c".asLeft }
            _         <- cache.clear
            _         <- released.complete(())
          } yield {}
        }
        .run()
    }


    test(s"getOrUpdateResource: $name") {
      cache
        .use { cache =>
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
          } yield {}
        }
        .run()
    }


    test(s"getOrUpdateResourceOpt: $name") {
      cache
        .use { cache =>
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
          } yield {}
        }
        .run()
    }


    test(s"getOrUpdateOpt1: $name") {
      cache
        .use { cache =>
          for {
            deferred <- Deferred[IO, Option[(Int, Option[IO[Unit]])]]
            value0   <- cache.getOrUpdateOpt1Ensure(0) { deferred.get }
            _        <- deferred.complete(none)
            value    <- value0.joinWithNever
            _        <- IO { value shouldEqual none[Int] }
            value    <- cache.getOrUpdateOpt1(0)((1, 1, none[IO[Unit]]).some.pure[IO])
            _        <- IO { value shouldEqual 1.asLeft.some }
          } yield {}
        }
      .run()
    }


    test(s"getOrUpdate1 does not fail after cache is released: $name") {
      val result = for {
        cache <- cache.use { _.pure[IO] }
        a     <- cache.getOrUpdate1(0)((1, 1, none[IO[Unit]]).pure[IO])
        _     <- IO { a shouldEqual 1.asLeft }
      } yield {}
      result.run()
    }

    test(s"getOrUpdate1 with release fails after cache is released: $name") {
      val result = for {
        cache <- cache.use { _.pure[IO] }
        a     <- cache.getOrUpdate1(0)((1, 1, IO.unit.some).pure[IO]).attempt
        _     <- IO { a shouldEqual CacheReleasedError.asLeft }
      } yield {}
      result.run()
    }

    test(s"getOrUpdateOpt1 does not fail after cache is released: $name") {
      val result = for {
        cache <- cache.use { _.pure[IO] }
        a     <- cache.getOrUpdateOpt1(0)((1, 1, none[IO[Unit]]).some.pure[IO])
        _     <- IO { a shouldEqual 1.asLeft.some }
      } yield {}
      result.run()
    }

    test(s"getOrUpdateOpt1 with release fails after cache is released: $name") {
      val result = for {
        cache <- cache.use { _.pure[IO] }
        a     <- cache.getOrUpdateOpt1(0)((1, 1, IO.unit.some).some.pure[IO]).attempt
        _     <- IO { a shouldEqual CacheReleasedError.asLeft }
      } yield {}
      result.run()
    }

    test(s"cancel getOrUpdate1: $name") {
      cache
        .use { cache =>
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
          } yield {}
        }
        .run()
    }


    test(s"put while getOrUpdate: $name") {
      cache
        .use { cache =>
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
          } yield {}
        }
        .run()
    }

    test(s"put while getOrUpdate1: $name") {
      cache
        .use { cache =>
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
          } yield {}
        }
        .run()
    }


    test(s"put while getOrUpdate never: $name") {
      cache
        .use { cache =>
          for {
            fiber <- cache.getOrUpdateEnsure(0) { IO.never[Int] }
            value <- cache.put(0, 0)
            value <- value
            _     <- IO { value shouldEqual none }
            value <- fiber.joinWithNever
            _     <- IO { value shouldEqual 0.asRight }
            value <- cache.get(0)
            _     <- IO { value shouldEqual 0.some }
          } yield {}
        }
        .run()
    }


    test(s"put while getOrUpdate1 never: $name") {
      cache
        .use { cache =>
          for {
            fiber <- cache.getOrUpdate1Ensure(0) { IO.never[(Int, Option[IO[Unit]])] }
            value <- cache.put(0, 0)
            value <- value
            _     <- IO { value shouldEqual none }
            value <- fiber.joinWithNever
            _     <- IO { value shouldEqual 0.asRight }
            value <- cache.get(0)
            _     <- IO { value shouldEqual 0.some }
          } yield {}
        }
      .run()
    }


    test(s"put while getOrUpdate failed: $name") {
      cache
        .use { cache =>
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
          } yield {}
        }
        .run()
    }


    test(s"put while getOrUpdate1 failed: $name") {
      cache
        .use { cache =>
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
          } yield {}
        }
        .run()
    }


    test(s"get while getOrUpdate: $name") {
      cache
        .use { cache =>
          for {
            deferred <- Deferred[IO, Int]
            value0   <- cache.getOrUpdateEnsure(0) { deferred.get }
            value1   <- cache.get(0).startEnsure
            _        <- deferred.complete(0)
            value    <- value0.joinWithNever
            _        <- IO { value shouldEqual 0.asRight }
            value    <- value1.joinWithNever
            _        <- IO { value shouldEqual 0.some }
          } yield {}
        }
        .run()
    }


    test(s"get while getOrUpdate1: $name") {
      cache
        .use { cache =>
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
          } yield {}
        }
        .run()
    }


    test(s"get while getOrUpdate failed: $name") {
      cache
        .use { cache =>
          for {
            deferred <- Deferred[IO, IO[Int]]
            value0   <- cache.getOrUpdateEnsure(0) { deferred.get.flatten }
            value1   <- cache.get(0).startEnsure
            _        <- deferred.complete(TestError.raiseError[IO, Int])
            value    <- value0.joinWithNever
            _        <- IO { value shouldEqual TestError.asLeft }
            value    <- value1.joinWithNever
            _        <- IO { value shouldEqual none[Int] }
          } yield {}
        }
        .run()
    }


    test(s"get while getOrUpdate1 failed: $name") {
      cache
        .use { cache =>
          for {
            deferred <- Deferred[IO, IO[(Int, Option[IO[Unit]])]]
            value0   <- cache.getOrUpdate1Ensure(0) { deferred.get.flatten }
            value1   <- cache.get(0).startEnsure
            _        <- deferred.complete(TestError.raiseError[IO, (Int, Option[IO[Unit]])])
            value    <- value0.joinWithNever
            _        <- IO { value shouldEqual TestError.asLeft }
            value    <- value1.joinWithNever
            _        <- IO { value shouldEqual none[Int] }
          } yield {}
        }
        .run()
    }


    test(s"remove while getOrUpdate: $name") {
      cache
        .use { cache =>
          for {
            deferred <- Deferred[IO, Int]
            value0   <- cache.getOrUpdateEnsure(0) { deferred.get }
            value1   <- cache.remove(0)
            _        <- deferred.complete(0)
            value    <- value0.joinWithNever
            _        <- IO { value shouldEqual 0.asRight }
            value    <- value1
            _        <- IO { value shouldEqual 0.some }
          } yield {}
        }
        .run()
    }


    test(s"remove while getOrUpdate1: $name") {
      cache
        .use { cache =>
          for {
            deferred <- Deferred[IO, (Int, Option[IO[Unit]])]
            fiber    <- cache.getOrUpdate1Ensure(0) { deferred.get }
            value1   <- cache.remove(0)
            release  <- Deferred[IO, Unit]
            released <- Ref[IO].of(false)
            _        <- deferred.complete((0, (release.get *> released.set(true)).some))
            value    <- fiber.joinWithNever
            _        <- IO { value shouldEqual 0.asRight }
            value    <- value1.startEnsure
            _        <- release.complete(())
            value    <- value.joinWithNever
            released <- released.get
            _        <- IO { released shouldEqual true }
            _        <- IO { value shouldEqual 0.some }
          } yield {}
        }
        .run()
    }


    test(s"remove while getOrUpdate failed: $name") {
      cache
        .use { cache =>
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
          } yield {}
        }
        .run()
    }


    test(s"remove while getOrUpdate1 failed: $name") {
      cache
        .use { cache =>
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
          } yield {}
        }
        .run()
    }


    test(s"clear while getOrUpdate: $name") {
      cache
        .use { cache =>
          for {
            deferred <- Deferred[IO, Int]
            value0   <- cache.getOrUpdateEnsure(0) { deferred.get }
            keys0    <- cache.keys
            _        <- cache.clear
            keys1    <- cache.keys
            _        <- deferred.complete(0)
            value0   <- value0.joinWithNever
            keys2    <- cache.keys
          } yield {
            keys0 shouldEqual Set(0)
            value0 shouldEqual 0.asRight
            keys1 shouldEqual Set.empty
            keys2 shouldEqual Set.empty
          }
        }
        .run()
    }


    test(s"clear while getOrUpdate1: $name") {
      cache
        .use { cache =>
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
          } yield {}
        }
        .run()
    }


    test(s"clear while getOrUpdate1 loading: $name") {
      cache
        .use { cache =>
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
          } yield {}
        }
        .run()
    }


    test(s"keys: $name") {
      cache
        .use { cache =>
          for {
            _     <- cache.put(0, 0)
            keys   = cache.keys
            keys0 <- keys
            _     <- cache.put(1, 1)
            keys1 <- keys
            _     <- cache.put(2, 2)
            keys2 <- keys
            _     <- cache.clear
            keys3 <- keys
          } yield {
            keys0 shouldEqual Set(0)
            keys1 shouldEqual Set(0, 1)
            keys2 shouldEqual Set(0, 1, 2)
            keys3 shouldEqual Set.empty
          }
        }
        .run()
    }


    test(s"values: $name") {
      cache
        .use { cache =>
          for {
            _       <- cache.put(0, 0)
            values   = cache.valuesFlatten
            values0 <- values
            _       <- cache.put(1, 1)
            values1 <- values
            _       <- cache.put(2, 2)
            values2 <- values
            _       <- cache.clear
            values3 <- values
          } yield {
            values0 shouldEqual Map((0, 0))
            values1 shouldEqual Map((0, 0), (1, 1))
            values2 shouldEqual Map((0, 0), (1, 1), (2, 2))
            values3 shouldEqual Map.empty
          }
        }
        .run()
    }

    test(s"values1: $name") {
      cache
        .use { cache =>
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
          } yield {}
        }
        .run()
    }

    test(s"cancellation: $name") {
      cache
        .use { cache =>
          for {
            deferred      <- Deferred[IO, Int]
            fiber         <- cache.getOrUpdateEnsure(0) { deferred.get }
            _             <- fiber.cancel.start
            _             <- deferred.complete(0)
            cancelOutcome <- fiber.join
            _             <- IO { cancelOutcome shouldEqual Outcome.canceled }
            value         <- cache.get(0)
          } yield {
            value shouldEqual 0.some
          }
        }
        .run()
    }

    ignore(s"cancellation proper: $name") {
      cache
        .use { cache =>
          for {
            deferred <- Deferred[IO, Int]
            fiber  <- cache.getOrUpdateEnsure(0)(deferred.get)
            fiber  <- fiber.cancel.start
            result <- cache.get(0)
            _      <- IO { result shouldEqual none }
            _      <- deferred.complete(0)
            _      <- fiber.joinWithNever
          } yield {}
        }
        .run()
    }

    test(s"no leak in case of failure: $name") {
      cache
        .use { cache =>
          for {
            result0 <- cache.getOrUpdate(0)(TestError.raiseError[IO, Int]).attempt
            result1 <- cache.getOrUpdate(0)(0.pure[IO]).attempt
            result2 <- cache.getOrUpdate(0)(TestError.raiseError[IO, Int]).attempt
          } yield {
            result0 shouldEqual TestError.asLeft[Int]
            result1 shouldEqual 0.asRight
            result2 shouldEqual 0.asRight
          }
        }
        .run()
    }

    test(s"foldMap: $name") {
      cache
        .use { cache =>
          for {
            _ <- cache.put(0, 1)
            expect = (a: Int) => for {
              b <- cache.foldMap { case (_, b) => b.map { _.pure[IO] }.merge }
              _ <- IO { b shouldEqual a }
            } yield {}
            _ <- expect(1)
            _ <- cache.put(1, 2)
            _ <- expect(3)
          } yield {}
        }
        .run()
    }

    test(s"foldMapPar: $name") {
      cache
        .use { cache =>
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
          } yield {}
        }
        .run()
    }
  }
}

object CacheSpec {

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
