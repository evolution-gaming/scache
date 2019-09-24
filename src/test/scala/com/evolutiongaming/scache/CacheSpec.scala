package com.evolutiongaming.scache

import cats.Monad
import cats.effect.concurrent.Deferred
import cats.effect.{Async, Concurrent, Fiber, IO, Resource, Sync}
import cats.effect.implicits._
import cats.implicits._
import com.evolutiongaming.scache.IOSuite._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.smetrics.CollectorRegistry
import org.scalatest.{AsyncFunSuite, Matchers}

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class CacheSpec extends AsyncFunSuite with Matchers {
  import CacheSpec._

  for {
    (name, cache0) <- List(
      ("default"               , Resource.liftF(Cache.loading[IO, Int, Int]())),
      ("no partitions"         , Resource.liftF(LoadingCache.of(LoadingCache.EntryRefs.empty[IO, Int, Int]))),
      ("expiring"              , Cache.expiring[IO, Int, Int](1.minute)),
      ("expiring no partitions", ExpiringCache.of[IO, Int, Int](1.minute)))
  } yield {

    val cache = for {
      cache   <- cache0
      metrics <- CacheMetrics.of(CollectorRegistry.empty[IO])
      cache   <- cache.withMetrics(metrics("name"))
    } yield cache

    
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


    test(s"put: $name") {
      val result = cache.use { cache =>
        for {
          value <- cache.put(0, 0)
          _     <- Sync[IO].delay { value shouldEqual none }
          value <- cache.get(0)
          _     <- Sync[IO].delay { value shouldEqual 0.some }
          value <- cache.put(0, 1)
          _     <- Sync[IO].delay { value shouldEqual 0.some }
          value <- cache.put(0, 2)
          _     <- Sync[IO].delay { value shouldEqual 1.some }
        } yield {}
      }
      result.run()
    }


    test(s"put releasable: $name") {
      val result = cache.use { cache =>
        for {
          release <- Deferred[IO, Unit]
          value   <- cache.put(0, 0, release.complete(()))
          _       <- Sync[IO].delay { value shouldEqual none }
          value   <- cache.get(0)
          _       <- Sync[IO].delay { value shouldEqual 0.some }
          value   <- cache.put(0, 1)
          _       <- release.get
          _       <- Sync[IO].delay { value shouldEqual 0.some }
          value   <- cache.put(0, 2)
          _       <- Sync[IO].delay { value shouldEqual 1.some }
        } yield {}
      }
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


    test(s"clear releasable: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, Unit]
          release  <- Deferred[IO, Unit]
          _        <- cache.put(0, 0, deferred.get *> release.complete(()))
          _        <- Deferred[IO, Unit]
          _        <- cache.put(1, 1, TestError.raiseError[IO, Unit])
          keys     <- cache.keys
          _        <- Sync[IO].delay { keys shouldEqual Set(0, 1) }
          result   <- cache.clear
          value    <- cache.get(0)
          _        <- Sync[IO].delay { value shouldEqual none[Int] }
          value    <- cache.get(1)
          _        <- Sync[IO].delay { value shouldEqual none[Int] }
          keys     <- cache.keys
          _        <- Sync[IO].delay { keys shouldEqual Set.empty }
          _        <- deferred.complete(())
          _        <- release.get
          _        <- result
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


    test(s"getOrUpdateReleasable: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, Releasable[IO, Int]]
          value0   <- cache.getOrUpdateReleasableEnsure(0) { deferred.get }
          value2   <- cache.getOrUpdateReleasable(0)(Releasable[IO].pure(1).pure[IO]).startEnsure
          _        <- deferred.complete(Releasable(0, Async[IO].never[Unit]))
          value    <- value0.join
          _        <- Sync[IO].delay { value shouldEqual 0 }
          value    <- value2.join
          _        <- Sync[IO].delay { value shouldEqual 0 }
        } yield {}
      }
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
          value0   <- cache.getOrUpdateReleasableEnsure(0) { deferred.get }
          value1   <- cache.get(0).startEnsure
          _        <- deferred.complete(Releasable(0, Async[IO].never[Unit]))
          value    <- value0.join
          _        <- Sync[IO].delay { value shouldEqual 0 }
          value    <- value1.join
          _        <- Sync[IO].delay { value shouldEqual 0.some }
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
          value0   <- cache.getOrUpdateReleasableEnsure(0) { deferred.get }
          value1   <- cache.remove(0)
          release  <- Deferred[IO, Unit]
          _        <- deferred.complete(Releasable(0, release.complete(())))
          value    <- value0.join
          _        <- Sync[IO].delay { value shouldEqual 0 }
          value    <- value1
          _        <- release.get
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
          deferred <- Deferred[IO, Unit]
          value    <- cache.getOrUpdateReleasable(0) { Releasable(0, deferred.complete(())).pure[IO] }
          keys     <- cache.keys
          _        <- Sync[IO].delay { keys shouldEqual Set(0) }
          _        <- cache.clear
          keys     <- cache.keys
          _        <- Sync[IO].delay { keys shouldEqual Set.empty }
          _        <- deferred.get
          _        <- Sync[IO].delay { value shouldEqual 0 }
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
          _        <- cache.clear
          keys     <- cache.keys
          _        <- Sync[IO].delay { keys shouldEqual Set.empty }
          release  <- Deferred[IO, Unit]
          _        <- deferred.complete(Releasable(0, release.complete(())))
          value    <- value.join
          _        <- Sync[IO].delay { value shouldEqual 0 }
          keys     <- cache.keys
          _        <- Sync[IO].delay { keys shouldEqual Set.empty }
          _        <- release.get
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

    def getOrUpdateReleasableEnsure(key: K)(value: => F[Releasable[F, V]])(implicit F: Concurrent[F]): F[Fiber[F, V]] = {
      for {
        deferred <- Deferred[F, Unit]
        fiber    <- self.getOrUpdateReleasable(key) { deferred.complete(()) *> value }.start
        _        <- deferred.get
      } yield fiber
    }
  }
}
