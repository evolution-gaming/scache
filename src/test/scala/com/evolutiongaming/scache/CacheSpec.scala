package com.evolutiongaming.scache

import cats.Monad
import cats.arrow.FunctionK
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Async, Concurrent, Fiber, IO, Sync}
import cats.effect.implicits._
import cats.syntax.all._
import com.evolutiongaming.scache.IOSuite._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.smetrics.CollectorRegistry

import scala.concurrent.duration._
import scala.util.control.NoStackTrace
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

class CacheSpec extends AsyncFunSuite with Matchers {
  import CacheSpec._

  for {
    (name, cache0) <- List(
      ("default"               , Cache.loading1[IO, Int, Int]),
      ("no partitions"         , LoadingCache.of(LoadingCache.EntryRefs.empty[IO, Int, Int])),
      ("expiring"              , Cache.expiring[IO, Int, Int](ExpiringCache.Config[IO, Int, Int](expireAfterRead = 1.minute))),
      ("expiring no partitions", ExpiringCache.of[IO, Int, Int](ExpiringCache.Config(expireAfterRead = 1.minute))))
  } yield {

    val cache = for {
      cache <- cache0
      metrics <- CacheMetrics.of(CollectorRegistry.empty[IO])
      cache <- cache.withMetrics(metrics("name"))
      cache <- cache
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
            _ <- f.join
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
        cache <- cache.use(_.pure[IO])
        a     <- cache.get(0)
        _      = a shouldEqual none
      } yield {}
      result.run()
    }


    test(s"getOrElse: $name") {
      cache
        .use { cache =>
          for {
            value <- cache.getOrElse(0, 1.pure[IO])
            _      = value shouldEqual 1
            _     <- cache.put(0, 2)
            value <- cache.getOrElse(0, 1.pure[IO])
            _      = value shouldEqual 2
          } yield {}
        }
        .run()
    }


    test(s"getOrElse succeeds after cache is released: $name") {
      val result = for {
        cache <- cache.use(_.pure[IO])
        a     <- cache.getOrElse(0, 1.pure[IO])
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
            _     <- Sync[IO].delay { value shouldEqual none }
            value <- cache.get(0)
            _     <- Sync[IO].delay { value shouldEqual 0.some }
            value <- cache.put(0, 1)
            value <- value
            _     <- Sync[IO].delay { value shouldEqual 0.some }
            value <- cache.put(0, 2)
            value <- value
            _     <- Sync[IO].delay { value shouldEqual 1.some }
          } yield {}
        }
        .run()
    }


    test(s"put succeeds after cache is released: $name") {
      val result = for {
        cache <- cache.use(_.pure[IO])
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
            _        <- Sync[IO].delay { value shouldEqual none }
            value    <- cache.get(0)
            _        <- Sync[IO].delay { value shouldEqual 0.some }
            value    <- cache.put(0, 1)
            _        <- release0.get
            _        <- release1.complete(())
            value    <- value
            _        <- Sync[IO].delay { value shouldEqual 0.some }
            value    <- released.get
            _        <- Sync[IO].delay { value shouldEqual true }
            value    <- cache.put(0, 2)
            value    <- value
            _        <- Sync[IO].delay { value shouldEqual 1.some }
          } yield {}
        }
        .run()
    }


    test(s"put releasable fails after cache is released: $name") {
      val result = for {
        cache <- cache.use(_.pure[IO])
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
            _     <- Sync[IO].delay { value shouldEqual 0.some }
            value <- cache.get(0)
            _     <- Sync[IO].delay { value shouldEqual none }
          } yield {}
        }
        .run()
    }


    test(s"remove succeeds after cache is released: $name") {
      val result = for {
        cache <- cache.use(_.pure[IO])
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
        cache <- cache.use(_.pure[IO])
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
            _         <- cache.getOrUpdateReleasable(2)(Releasable(2, release.get *> released1.set(true)).pure[IO])
            _         <- cache.getOrUpdateReleasable(3)(Releasable(3, TestError.raiseError[IO, Unit]).pure[IO])
            keys      <- cache.keys
            _         <- Sync[IO].delay { keys shouldEqual Set(0, 1, 2, 3) }
            clear     <- cache.clear
            _ <- keys.toList.foldMapM { key =>
              for {
                value    <- cache.get(key)
                _        <- Sync[IO].delay { value shouldEqual none[Int] }
              } yield {}
            }
            keys      <- cache.keys
            _         <- Sync[IO].delay { keys shouldEqual Set.empty }
            _         <- release.complete(()).start
            _         <- clear
            value     <- released0.get
            _         <- Sync[IO].delay { value shouldEqual true }
            value     <- released1.get
            _         <- Sync[IO].delay { value shouldEqual true }
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
        cache <- cache.use(_.pure[IO])
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


    test(s"getOrUpdateReleasable: $name") {
      cache
        .use { cache =>
          for {
            deferred <- Deferred[IO, Releasable[IO, Int]]
            value0   <- cache.getOrUpdateReleasableEnsure(0) { deferred.get }
            value2   <- cache.getOrUpdateReleasable(0)(Releasable[IO].pure(1).pure[IO]).startEnsure
            released <- Deferred[IO, Unit]
            _        <- deferred.complete(Releasable(0, released.get))
            value    <- value0.joinWithNever
            _        <- Sync[IO].delay { value shouldEqual 0.asRight }
            value    <- value2.joinWithNever
            _        <- Sync[IO].delay { value shouldEqual 0 }
            _        <- released.complete(())
          } yield {}
        }
        .run()
    }


    test(s"getOrUpdateReleasableOpt: $name") {
      cache
        .use { cache =>
          for {
            deferred <- Deferred[IO, Option[Releasable[IO, Int]]]
            value0   <- cache.getOrUpdateReleasableOptEnsure(0) { deferred.get }
            _        <- deferred.complete(none)
            value    <- value0.join
            _        <- Sync[IO].delay { value shouldEqual Outcome.succeeded(IO.pure(none[Int])) }
            value    <- cache.getOrUpdateReleasableOpt(0)(Releasable[IO].pure(1).some.pure[IO])
            _        <- Sync[IO].delay { value shouldEqual 1.some }
          } yield {}
        }
      .run()
    }


    test(s"getOrUpdateReleasable fails after cache is released: $name") {
      val result = for {
        cache <- cache.use(_.pure[IO])
        a     <- cache.getOrUpdateReleasable(0)(Releasable[IO].pure(1).pure[IO]).attempt
        _      = a shouldEqual CacheReleasedError.asLeft
      } yield {}
      result.run()
    }

    test(s"getOrUpdateReleasableOpt fails after cache is released: $name") {
      val result = for {
        cache <- cache.use(_.pure[IO])
        a     <- cache.getOrUpdateReleasableOpt(0)(Releasable[IO].pure(1).some.pure[IO]).attempt
        _      = a shouldEqual CacheReleasedError.asLeft
      } yield {}
      result.run()
    }


    test(s"cancel getOrUpdateReleasable: $name") {
      cache
        .use { cache =>
          for {
            deferred0     <- Deferred[IO, Releasable[IO, Int]]
            fiber0        <- cache.getOrUpdateReleasableEnsure(0) { deferred0.get }
            fiber1        <- cache.getOrUpdateReleasable(0) { Async[IO].never }.startEnsure
            release       <- Deferred[IO, Unit]
            _             <- fiber0.cancel.start
            _             <- deferred0.complete(Releasable(0, release.complete(()).void))
            cancelOutcome <- fiber0.join
            _             <- Sync[IO].delay { cancelOutcome shouldEqual Outcome.canceled }
            value         <- fiber1.join
            _             <- Sync[IO].delay { value shouldEqual Outcome.succeeded(IO.pure(0)) }
            _             <- cache.remove(0)
            _             <- release.get
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
            _        <- Sync[IO].delay { value shouldEqual none }
            value    <- fiber.joinWithNever
            _        <- Sync[IO].delay { value shouldEqual 1.asRight }
            value    <- cache.get(0)
            _        <- Sync[IO].delay { value shouldEqual 1.some }
          } yield {}
        }
        .run()
    }


    test(s"put while getOrUpdateReleasable: $name") {
      cache
        .use { cache =>
          for {
            deferred <- Deferred[IO, Releasable[IO, Int]]
            fiber    <- cache.getOrUpdateReleasableEnsure(0) { deferred.get }
            value    <- cache.put(0, 1)
            release  <- Deferred[IO, Unit]
            _        <- deferred.complete(Releasable(0, release.complete(()).void))
            _        <- release.get
            value    <- value
            _        <- Sync[IO].delay { value shouldEqual none }
            value    <- fiber.joinWithNever
            _        <- Sync[IO].delay { value shouldEqual 1.asRight }
            value    <- cache.get(0)
            _        <- Sync[IO].delay { value shouldEqual 1.some }
          } yield {}
        }
        .run()
    }


    test(s"put while getOrUpdate never: $name") {
      cache
        .use { cache =>
          for {
            fiber    <- cache.getOrUpdateEnsure(0) { Async[IO].never[Int] }
            value    <- cache.put(0, 0)
            value    <- value
            _        <- Sync[IO].delay { value shouldEqual none }
            value    <- fiber.joinWithNever
            _        <- Sync[IO].delay { value shouldEqual 0.asRight }
            value    <- cache.get(0)
            _        <- Sync[IO].delay { value shouldEqual 0.some }
          } yield {}
        }
        .run()
    }


    test(s"put while getOrUpdateReleasable never: $name") {
      cache
        .use { cache =>
          for {
            fiber    <- cache.getOrUpdateReleasableEnsure(0) { Async[IO].never[Releasable[IO, Int]] }
            value    <- cache.put(0, 0)
            value    <- value
            _        <- Sync[IO].delay { value shouldEqual none }
            value    <- fiber.joinWithNever
            _        <- Sync[IO].delay { value shouldEqual 0.asRight }
            value    <- cache.get(0)
            _        <- Sync[IO].delay { value shouldEqual 0.some }
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
            _        <- Sync[IO].delay { value shouldEqual none }
            value    <- fiber.joinWithNever
            _        <- Sync[IO].delay { value shouldEqual 1.asRight }
            value    <- cache.get(0)
            _        <- Sync[IO].delay { value shouldEqual 1.some }
          } yield {}
        }
        .run()
    }


    test(s"put while getOrUpdateReleasable failed: $name") {
      cache
        .use { cache =>
          for {
            deferred <- Deferred[IO, IO[Releasable[IO, Int]]]
            fiber    <- cache.getOrUpdateReleasableEnsure(0) { deferred.get.flatten }
            value    <- cache.put(0, 1)
            _        <- deferred.complete(TestError.raiseError[IO, Releasable[IO, Int]])
            value    <- value
            _        <- Sync[IO].delay { value shouldEqual none }
            value    <- fiber.joinWithNever
            _        <- Sync[IO].delay { value shouldEqual 1.asRight }
            value    <- cache.get(0)
            _        <- Sync[IO].delay { value shouldEqual 1.some }
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
            _        <- Sync[IO].delay { value shouldEqual 0.asRight }
            value    <- value1.joinWithNever
            _        <- Sync[IO].delay { value shouldEqual 0.some }
          } yield {}
        }
        .run()
    }


    test(s"get while getOrUpdateReleasable: $name") {
      cache
        .use { cache =>
          for {
            deferred <- Deferred[IO, Releasable[IO, Int]]
            released <- Deferred[IO, Unit]
            value0   <- cache.getOrUpdateReleasableEnsure(0) { deferred.get }
            value1   <- cache.get(0).startEnsure
            _        <- deferred.complete(Releasable(0, released.get))
            value    <- value0.joinWithNever
            _        <- Sync[IO].delay { value shouldEqual 0.asRight }
            value    <- value1.joinWithNever
            _        <- Sync[IO].delay { value shouldEqual 0.some }
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
            _        <- Sync[IO].delay { value shouldEqual TestError.asLeft }
            value    <- value1.joinWithNever
            _        <- Sync[IO].delay { value shouldEqual none[Int] }
          } yield {}
        }
        .run()
    }


    test(s"get while getOrUpdateReleasable failed: $name") {
      cache
        .use { cache =>
          for {
            deferred <- Deferred[IO, IO[Releasable[IO, Int]]]
            value0   <- cache.getOrUpdateReleasableEnsure(0) { deferred.get.flatten }
            value1   <- cache.get(0).startEnsure
            _        <- deferred.complete(TestError.raiseError[IO, Releasable[IO, Int]])
            value    <- value0.joinWithNever
            _        <- Sync[IO].delay { value shouldEqual TestError.asLeft }
            value    <- value1.joinWithNever
            _        <- Sync[IO].delay { value shouldEqual none[Int] }
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
            _        <- Sync[IO].delay { value shouldEqual 0.asRight }
            value    <- value1
            _        <- Sync[IO].delay { value shouldEqual 0.some }
          } yield {}
        }
        .run()
    }


    test(s"remove while getOrUpdateReleasable: $name") {
      cache
        .use { cache =>
          for {
            deferred <- Deferred[IO, Releasable[IO, Int]]
            fiber    <- cache.getOrUpdateReleasableEnsure(0) { deferred.get }
            value1   <- cache.remove(0)
            release  <- Deferred[IO, Unit]
            released <- Ref[IO].of(false)
            _        <- deferred.complete(Releasable(0, release.get *> released.set(true)))
            value    <- fiber.joinWithNever
            _        <- Sync[IO].delay { value shouldEqual 0.asRight }
            value    <- value1.startEnsure
            _        <- release.complete(())
            value    <- value.joinWithNever
            released <- released.get
            _        <- Sync[IO].delay { released shouldEqual true }
            _        <- Sync[IO].delay { value shouldEqual 0.some }
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
            _        <- Sync[IO].delay { value shouldEqual none }
            value    <- fiber.joinWithNever
            _        <- Sync[IO].delay { value shouldEqual TestError.asLeft }
            value    <- cache.get(0)
            _        <- Sync[IO].delay { value shouldEqual none }
          } yield {}
        }
        .run()
    }


    test(s"remove while getOrUpdateReleasable failed: $name") {
      cache
        .use { cache =>
          for {
            deferred <- Deferred[IO, IO[Releasable[IO, Int]]]
            fiber    <- cache.getOrUpdateReleasableEnsure(0) { deferred.get.flatten }
            value    <- cache.remove(0)
            _        <- deferred.complete(TestError.raiseError[IO, Releasable[IO, Int]])
            value    <- value
            _        <- Sync[IO].delay { value shouldEqual none }
            value    <- fiber.joinWithNever
            _        <- Sync[IO].delay { value shouldEqual TestError.asLeft }
            value    <- cache.get(0)
            _        <- Sync[IO].delay { value shouldEqual none }
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


    test(s"clear while getOrUpdateReleasable: $name") {
      cache
        .use { cache =>
          for {
            release  <- Deferred[IO, Unit]
            released <- Ref[IO].of(false)
            value    <- cache.getOrUpdateReleasable(0) { Releasable(0, release.get *> released.set(true)).pure[IO] }
            _        <- Sync[IO].delay { value shouldEqual 0 }
            keys     <- cache.keys
            _        <- Sync[IO].delay { keys shouldEqual Set(0) }
            clear    <- cache.clear
            keys     <- cache.keys
            _        <- Sync[IO].delay { keys shouldEqual Set.empty }
            _        <- release.complete(())
            _        <- clear
            released <- released.get
            _        <- Sync[IO].delay { released shouldEqual true }
            keys     <- cache.keys
            _        <- Sync[IO].delay { keys shouldEqual Set.empty }
          } yield {}
        }
        .run()
    }


    test(s"clear while getOrUpdateReleasable loading: $name") {
      cache
        .use { cache =>
          for {
            deferred <- Deferred[IO, Releasable[IO, Int]]
            value    <- cache.getOrUpdateReleasableEnsure(0) { deferred.get }
            keys     <- cache.keys
            _        <- Sync[IO].delay { keys shouldEqual Set(0) }
            clear    <- cache.clear
            keys     <- cache.keys
            _        <- Sync[IO].delay { keys shouldEqual Set.empty }
            release  <- Deferred[IO, Unit]
            released <- Ref[IO].of(false)
            _        <- deferred.complete(Releasable(0, release.get *> released.set(true)))
            value    <- value.joinWithNever
            _        <- Sync[IO].delay { value shouldEqual 0.asRight }
            keys     <- cache.keys
            _        <- Sync[IO].delay { keys shouldEqual Set.empty }
            _        <- release.complete(())
            _        <- clear
            released <- released.get
            _        <- Sync[IO].delay { released shouldEqual true }
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
            _             <- Sync[IO].delay { cancelOutcome shouldEqual Outcome.canceled }
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
            fiber  <- cache.getOrUpdateOptEnsure(0)(IO.sleep(10.seconds).as(0.some))
            _      <- fiber.cancel
            result <- cache.get(0)
            _      <- IO { result shouldEqual none }
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
            _ <- IO { a shouldEqual 6 }
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


    def getOrUpdateEnsure(key: K)(value: => F[V])(implicit F: Concurrent[F]): F[Fiber[F, Either[Throwable, V]]] = {
      for {
        d <- Deferred[F, Unit]
        f <- self
          .getOrUpdate(key) {
            for {
              _ <- d.complete(())
              a <- value
            } yield a
          }
          .attempt
          .start
        _ <- d.get
      } yield f
    }

    def getOrUpdateOptEnsure(key: K)(value: => F[Option[V]])(implicit F: Concurrent[F]): F[Fiber[F, Either[Throwable, Option[V]]]] = {
      for {
        d <- Deferred[F, Unit]
        f <- self
          .getOrUpdateOpt(key) {
            for {
              _ <- d.complete(())
              a <- value
            } yield a
          }
          .attempt
          .start
        _ <- d.get
      } yield f
    }

    def getOrUpdateReleasableEnsure(key: K)(value: => F[Releasable[F, V]])(implicit F: Concurrent[F]): F[Fiber[F, Either[Throwable, V]]] = {
      for {
        d <- Deferred[F, Unit]
        f <- self
          .getOrUpdateReleasable(key) { d.complete(()) *> value }
          .attempt
          .start
        _ <- d.get
      } yield f
    }

    def getOrUpdateReleasableOptEnsure(
      key: K)(
      value: => F[Option[Releasable[F, V]]])(implicit
      F: Concurrent[F]
    ): F[Fiber[F, Option[V]]] = {
      for {
        d <- Deferred[F, Unit]
        f <- self
          .getOrUpdateReleasableOpt(key) { d.complete(()) *> value }
          .start
        _ <- d.get
      } yield f
    }
  }
}
