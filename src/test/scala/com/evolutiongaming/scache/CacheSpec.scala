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
      ("default"               , Cache.loading[IO, Int, Int]),
      ("no partitions"         , LoadingCache.of(LoadingCache.EntryRefs.empty[IO, Int, Int])),
      ("expiring"              , Cache.expiring[IO, Int, Int](ExpiringCache.Config[IO, Int, Int](expireAfterRead = 1.minute))),
      ("expiring no partitions", ExpiringCache.of[IO, Int, Int](ExpiringCache.Config(expireAfterRead = 1.minute))))
  } yield {

    val cache = {
      val cache = for {
        cache   <- cache0
        metrics <- CacheMetrics.of(CollectorRegistry.empty[IO])
        cache   <- cache.withMetrics(metrics("name"))
      } yield {
        cache.mapK(FunctionK.id, FunctionK.id)
      }
      cache.withFence
    }

    test(s"get: $name") {
      val result = cache.use { cache =>
        for {
          value <- cache.get(0)
        } yield {
          value shouldEqual none[Int]
        }
      }
      result.run()
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
      val result = cache.use { cache =>
        for {
          value <- cache.getOrElse(0, 1.pure[IO])
          _      = value shouldEqual 1
          _     <- cache.put(0, 2)
          value <- cache.getOrElse(0, 1.pure[IO])
          _      = value shouldEqual 2
        } yield {}
      }
      result.run()
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
      val result = cache.use { cache =>
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
      result.run()
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
      val result = cache.use { cache =>
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
      result.run()
    }


    test(s"put releasable fails after cache is released: $name") {
      val result = for {
        cache <- cache.use(_.pure[IO])
        a     <- cache.put(0, 0, ().pure[IO]).flatten.attempt
        _      = a shouldEqual CacheReleasedError.asLeft
      } yield {}
      result.run()
    }


    test(s"size: $name") {
      val result = cache.use { cache =>
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
      result.run()
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
      val result = cache.use { cache =>
        for {
          _     <- cache.put(0, 0)
          value <- cache.remove(0)
          value <- value
          _     <- Sync[IO].delay { value shouldEqual 0.some }
          value <- cache.get(0)
          _     <- Sync[IO].delay { value shouldEqual none }
        } yield {}
      }
      result.run()
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
      val result = cache.use { cache =>
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
      result.run()
    }


    test(s"clear succeeds after cache is released: $name") {
      val result = for {
        cache <- cache.use(_.pure[IO])
        _     <- cache.clear.flatten
      } yield {}
      result.run()
    }


    test(s"clear releasable: $name") {
      val result = cache.use { cache =>
        for {
          release   <- Deferred[IO, Unit]
          released0 <- Ref[IO].of(false)
          _         <- cache.put(0, 0, release.get *> released0.set(true))
          _         <- Deferred[IO, Unit]
          _         <- cache.put(1, 1, TestError.raiseError[IO, Unit])
          released1 <- Ref[IO].of(false)
          _         <- cache.getOrUpdateReleasable(2)(Releasable(2, release.get *> released1.set(true)).pure[IO])
          _         <- Deferred[IO, Unit]
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
      result.run()
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

      val result = cache.use { cache =>
        for {
          result0 <- getAll(cache)
          _       <- values.foldMapM { key => cache.put(key, key).void }
          result1 <- getAll(cache)
        } yield {
          result0.flatten.reverse shouldEqual List.empty
          result1.flatten.reverse shouldEqual values
        }
      }
      result.run()
    }


    test(s"getOrUpdate: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, Int]
          value0   <- cache.getOrUpdateEnsure(0) { deferred.get }
          value2   <- cache.getOrUpdate(0)(1.pure[IO]).startEnsure
          _        <- deferred.complete(0)
          value0   <- value0.join
          value1   <- value2.join
        } yield {
          value0 shouldEqual 0
          value1 shouldEqual 0
        }
      }
      result.run()
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
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, Option[Int]]
          value0   <- cache.getOrUpdateOptEnsure(0) { deferred.get }
          value1   <- cache.getOrUpdateOpt(0)(0.some.pure[IO]).startEnsure
          _        <- deferred.complete(none)
          value0   <- value0.join
          value1   <- value1.join
          _         = value0 shouldEqual none
          _         = value1 shouldEqual none
          value    <- cache.getOrUpdateOpt(0)(0.some.pure[IO])
          _         = value shouldEqual 0.some
        } yield {}
      }
      result.run()
    }


    test(s"getOrUpdateReleasable: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, Releasable[IO, Int]]
          value0   <- cache.getOrUpdateReleasableEnsure(0) { deferred.get }
          value2   <- cache.getOrUpdateReleasable(0)(Releasable[IO].pure(1).pure[IO]).startEnsure
          released <- Deferred[IO, Unit]
          _        <- deferred.complete(Releasable(0, released.get))
          value    <- value0.join
          _        <- Sync[IO].delay { value shouldEqual 0 }
          value    <- value2.join
          _        <- Sync[IO].delay { value shouldEqual 0 }
          _        <- released.complete(())
        } yield {}
      }
      result.run()
    }


    test(s"getOrUpdateReleasableOpt: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, Option[Releasable[IO, Int]]]
          value0   <- cache.getOrUpdateReleasableOptEnsure(0) { deferred.get }
          value1   <- cache.getOrUpdateReleasableOpt(0)(Releasable[IO].pure(1).some.pure[IO]).startEnsure
          released <- Deferred[IO, Unit]
          _        <- deferred.complete(none)
          value    <- value0.join
          _        <- Sync[IO].delay { value shouldEqual none }
          value    <- value1.join
          _        <- Sync[IO].delay { value shouldEqual none }
          _        <- released.complete(())
          value    <- cache.getOrUpdateReleasableOpt(0)(Releasable[IO].pure(1).some.pure[IO])
          _        <- Sync[IO].delay { value shouldEqual 1.some }
        } yield {}
      }
      result.run()
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
      val result = cache.use { cache =>
        for {
          deferred0 <- Deferred[IO, Releasable[IO, Int]]
          fiber0    <- cache.getOrUpdateReleasableEnsure(0) { deferred0.get }
          fiber1    <- cache.getOrUpdateReleasable(0) { Async[IO].never }.startEnsure
          release   <- Deferred[IO, Unit]
          _         <- fiber0.cancel
          _         <- deferred0.complete(Releasable(0, release.complete(())))
          value     <- fiber1.join
          _         <- Sync[IO].delay { value shouldEqual 0 }
          _         <- cache.remove(0)
          _         <- release.get
        } yield {}
      }
      result.run()
    }


    test(s"put while getOrUpdate: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, Int]
          fiber    <- cache.getOrUpdateEnsure(0) { deferred.get }
          value    <- cache.put(0, 1)
          _        <- deferred.complete(0)
          value    <- value
          _        <- Sync[IO].delay { value shouldEqual none }
          value    <- fiber.join
          _        <- Sync[IO].delay { value shouldEqual 1 }
          value    <- cache.get(0)
          _        <- Sync[IO].delay { value shouldEqual 1.some }
        } yield {}
      }
      result.run()
    }


    test(s"put while getOrUpdateReleasable: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, Releasable[IO, Int]]
          fiber    <- cache.getOrUpdateReleasableEnsure(0) { deferred.get }
          value    <- cache.put(0, 1)
          release  <- Deferred[IO, Unit]
          _        <- deferred.complete(Releasable(0, release.complete(())))
          _        <- release.get
          value    <- value
          _        <- Sync[IO].delay { value shouldEqual none }
          value    <- fiber.join
          _        <- Sync[IO].delay { value shouldEqual 1 }
          value    <- cache.get(0)
          _        <- Sync[IO].delay { value shouldEqual 1.some }
        } yield {}
      }
      result.run()
    }


    test(s"put while getOrUpdate never: $name") {
      val result = cache.use { cache =>
        for {
          fiber    <- cache.getOrUpdateEnsure(0) { Async[IO].never[Int] }
          value    <- cache.put(0, 0)
          value    <- value
          _        <- Sync[IO].delay { value shouldEqual none }
          value    <- fiber.join
          _        <- Sync[IO].delay { value shouldEqual 0 }
          value    <- cache.get(0)
          _        <- Sync[IO].delay { value shouldEqual 0.some }
        } yield {}
      }
      result.run()
    }


    test(s"put while getOrUpdateReleasable never: $name") {
      val result = cache.use { cache =>
        for {
          fiber    <- cache.getOrUpdateReleasableEnsure(0) { Async[IO].never[Releasable[IO, Int]] }
          value    <- cache.put(0, 0)
          value    <- value
          _        <- Sync[IO].delay { value shouldEqual none }
          value    <- fiber.join
          _        <- Sync[IO].delay { value shouldEqual 0 }
          value    <- cache.get(0)
          _        <- Sync[IO].delay { value shouldEqual 0.some }
        } yield {}
      }
      result.run()
    }


    test(s"put while getOrUpdate failed: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, IO[Int]]
          fiber    <- cache.getOrUpdateEnsure(0) { deferred.get.flatten }
          value    <- cache.put(0, 1)
          _        <- deferred.complete(TestError.raiseError[IO, Int])
          value    <- value
          _        <- Sync[IO].delay { value shouldEqual none }
          value    <- fiber.join
          _        <- Sync[IO].delay { value shouldEqual 1 }
          value    <- cache.get(0)
          _        <- Sync[IO].delay { value shouldEqual 1.some }
        } yield {}
      }
      result.run()
    }


    test(s"put while getOrUpdateReleasable failed: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, IO[Releasable[IO, Int]]]
          fiber    <- cache.getOrUpdateReleasableEnsure(0) { deferred.get.flatten }
          value    <- cache.put(0, 1)
          _        <- deferred.complete(TestError.raiseError[IO, Releasable[IO, Int]])
          value    <- value
          _        <- Sync[IO].delay { value shouldEqual none }
          value    <- fiber.join
          _        <- Sync[IO].delay { value shouldEqual 1 }
          value    <- cache.get(0)
          _        <- Sync[IO].delay { value shouldEqual 1.some }
        } yield {}
      }
      result.run()
    }


    test(s"get while getOrUpdate: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, Int]
          value0   <- cache.getOrUpdateEnsure(0) { deferred.get }
          value1   <- cache.get(0).startEnsure
          _        <- deferred.complete(0)
          value    <- value0.join
          _        <- Sync[IO].delay { value shouldEqual 0 }
          value    <- value1.join
          _        <- Sync[IO].delay { value shouldEqual 0.some }
        } yield {}
      }
      result.run()
    }


    test(s"get while getOrUpdateReleasable: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, Releasable[IO, Int]]
          released <- Deferred[IO, Unit]
          value0   <- cache.getOrUpdateReleasableEnsure(0) { deferred.get }
          value1   <- cache.get(0).startEnsure
          _        <- deferred.complete(Releasable(0, released.get))
          value    <- value0.join
          _        <- Sync[IO].delay { value shouldEqual 0 }
          value    <- value1.join
          _        <- Sync[IO].delay { value shouldEqual 0.some }
          _        <- released.complete(())
        } yield {}
      }
      result.run()
    }


    test(s"get while getOrUpdate failed: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, IO[Int]]
          value0   <- cache.getOrUpdateEnsure(0) { deferred.get.flatten }
          value1   <- cache.get(0).startEnsure
          _        <- deferred.complete(TestError.raiseError[IO, Int])
          value    <- value0.join.attempt
          _        <- Sync[IO].delay { value shouldEqual TestError.asLeft }
          value    <- value1.join
          _        <- Sync[IO].delay { value shouldEqual none }
        } yield {}
      }
      result.run()
    }


    test(s"get while getOrUpdateReleasable failed: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, IO[Releasable[IO, Int]]]
          value0   <- cache.getOrUpdateReleasableEnsure(0) { deferred.get.flatten }
          value1   <- cache.get(0).startEnsure
          _        <- deferred.complete(TestError.raiseError[IO, Releasable[IO, Int]])
          value    <- value0.join.attempt
          _        <- Sync[IO].delay { value shouldEqual TestError.asLeft }
          value    <- value1.join
          _        <- Sync[IO].delay { value shouldEqual none }
        } yield {}
      }
      result.run()
    }


    test(s"remove while getOrUpdate: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, Int]
          value0   <- cache.getOrUpdateEnsure(0) { deferred.get }
          value1   <- cache.remove(0)
          _        <- deferred.complete(0)
          value    <- value0.join
          _        <- Sync[IO].delay { value shouldEqual 0 }
          value    <- value1
          _        <- Sync[IO].delay { value shouldEqual 0.some }
        } yield {}
      }
      result.run()
    }


    test(s"remove while getOrUpdateReleasable: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, Releasable[IO, Int]]
          fiber    <- cache.getOrUpdateReleasableEnsure(0) { deferred.get }
          value1   <- cache.remove(0)
          release  <- Deferred[IO, Unit]
          released <- Ref[IO].of(false)
          _        <- deferred.complete(Releasable(0, release.get *> released.set(true)))
          value    <- fiber.join
          _        <- Sync[IO].delay { value shouldEqual 0 }
          value    <- value1.startEnsure
          _        <- release.complete(())
          value    <- value.join
          released <- released.get
          _        <- Sync[IO].delay { released shouldEqual true }
          _        <- Sync[IO].delay { value shouldEqual 0.some }
        } yield {}
      }
      result.run()
    }


    test(s"remove while getOrUpdate failed: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, IO[Int]]
          fiber    <- cache.getOrUpdateEnsure(0) { deferred.get.flatten }
          value    <- cache.remove(0)
          _        <- deferred.complete(TestError.raiseError[IO, Int])
          value    <- value
          _        <- Sync[IO].delay { value shouldEqual none }
          value    <- fiber.join.attempt
          _        <- Sync[IO].delay { value shouldEqual TestError.asLeft }
          value    <- cache.get(0)
          _        <- Sync[IO].delay { value shouldEqual none }
        } yield {}
      }
      result.run()
    }


    test(s"remove while getOrUpdateReleasable failed: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, IO[Releasable[IO, Int]]]
          fiber    <- cache.getOrUpdateReleasableEnsure(0) { deferred.get.flatten }
          value    <- cache.remove(0)
          _        <- deferred.complete(TestError.raiseError[IO, Releasable[IO, Int]])
          value    <- value
          _        <- Sync[IO].delay { value shouldEqual none }
          value    <- fiber.join.attempt
          _        <- Sync[IO].delay { value shouldEqual TestError.asLeft }
          value    <- cache.get(0)
          _        <- Sync[IO].delay { value shouldEqual none }
        } yield {}
      }
      result.run()
    }


    test(s"clear while getOrUpdate: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, Int]
          value0   <- cache.getOrUpdateEnsure(0) { deferred.get }
          keys0    <- cache.keys
          _        <- cache.clear
          keys1    <- cache.keys
          _        <- deferred.complete(0)
          value0   <- value0.join
          keys2    <- cache.keys
        } yield {
          keys0 shouldEqual Set(0)
          value0 shouldEqual 0
          keys1 shouldEqual Set.empty
          keys2 shouldEqual Set.empty
        }
      }
      result.run()
    }


    test(s"clear while getOrUpdateReleasable: $name") {
      val result = cache.use { cache =>
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
      result.run()
    }


    test(s"clear while getOrUpdateReleasable loading: $name") {
      val result = cache.use { cache =>
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
          value    <- value.join
          _        <- Sync[IO].delay { value shouldEqual 0 }
          keys     <- cache.keys
          _        <- Sync[IO].delay { keys shouldEqual Set.empty }
          _        <- release.complete(())
          _        <- clear
          released <- released.get
          _        <- Sync[IO].delay { released shouldEqual true }
        } yield {}
      }
      result.run()
    }


    test(s"keys: $name") {
      val result = cache.use { cache =>
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

      result.run()
    }


    test(s"values: $name") {
      val result = cache.use { cache =>
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
      result.run()
    }


    test(s"cancellation: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, Int]
          fiber    <- cache.getOrUpdateEnsure(0) { deferred.get }
          _        <- fiber.cancel
          _        <- deferred.complete(0)
          value    <- cache.get(0)
        } yield {
          value shouldEqual 0.some
        }
      }
      result.run()
    }


    test(s"no leak in case of failure: $name") {
      val result = cache.use { cache =>
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
      result.run()
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


    def getOrUpdateEnsure(key: K)(value: => F[V])(implicit F: Concurrent[F]): F[Fiber[F, V]] = {

      def getOrUpdate(deferred: Deferred[F, Unit]) = {
        self.getOrUpdate(key) {
          for {
            _     <- deferred.complete(())
            value <- value
          } yield value
        }
      }

      for {
        deferred <- Deferred[F, Unit]
        fiber    <- getOrUpdate(deferred).start
        _        <- deferred.get
      } yield fiber
    }

    def getOrUpdateOptEnsure(key: K)(value: => F[Option[V]])(implicit F: Concurrent[F]): F[Fiber[F, Option[V]]] = {

      def getOrUpdateOpt(deferred: Deferred[F, Unit]) = {
        self.getOrUpdateOpt(key) {
          for {
            _     <- deferred.complete(())
            value <- value
          } yield value
        }
      }

      for {
        deferred <- Deferred[F, Unit]
        fiber    <- getOrUpdateOpt(deferred).start
        _        <- deferred.get
      } yield fiber
    }

    def getOrUpdateReleasableEnsure(key: K)(value: => F[Releasable[F, V]])(implicit F: Concurrent[F]): F[Fiber[F, V]] = {
      for {
        deferred <- Deferred[F, Unit]
        fiber    <- self.getOrUpdateReleasable(key) { deferred.complete(()) *> value }.start
        _        <- deferred.get
      } yield fiber
    }

    def getOrUpdateReleasableOptEnsure(
      key: K)(
      value: => F[Option[Releasable[F, V]]])(implicit
      F: Concurrent[F]
    ): F[Fiber[F, Option[V]]] = {
      for {
        deferred <- Deferred[F, Unit]
        fiber    <- self.getOrUpdateReleasableOpt(key) { deferred.complete(()) *> value }.start
        _        <- deferred.get
      } yield fiber
    }
  }
}
