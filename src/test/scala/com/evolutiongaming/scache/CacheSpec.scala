package com.evolutiongaming.scache

import cats.Monad
import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, Fiber, IO, Resource}
import cats.implicits._
import com.evolutiongaming.scache.IOSuite._
import org.scalatest.{AsyncFunSuite, Matchers}

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class CacheSpec extends AsyncFunSuite with Matchers {
  import CacheSpec._

  for {
    (name, cache) <- List(
      ("default"               , Resource.liftF(Cache.of[IO, Int, Int])),
      ("no partitions"         , Resource.liftF(LoadingCache.of(LoadingCache.EntryRefs.empty[IO, Int, Int]))),
      ("expiring"              , Cache.of[IO, Int, Int](1.minute)),
      ("expiring no partitions", ExpiringCache.of[IO, Int, Int](1.minute)))
  } yield {

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
          value0 <- cache.put(0, 0)
          value0 <- value0.swap
          value1 <- cache.get(0)
          value2 <- cache.put(0, 1)
          value2 <- value2.swap
          value3 <- cache.put(0, 2)
          value3 <- value3.swap
        } yield {
          value0 shouldEqual none
          value1 shouldEqual 0.some
          value2 shouldEqual 0.some
          value3 shouldEqual 1.some
        }
      }
      result.run()
    }


    test(s"remove: $name") {
      val result = cache.use { cache =>
        for {
          _      <- cache.put(0, 0)
          value0 <- cache.remove(0)
          value0 <- value0.swap
          value1 <- cache.get(0)
        } yield {
          value0 shouldEqual 0.some
          value1 shouldEqual none
        }
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
          value2   <- Concurrent[IO].startEnsure { cache.getOrUpdate(0)(1.pure[IO]) }
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


    test(s"put while getOrUpdate: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, Int]
          fiber    <- cache.getOrUpdateEnsure(0) { deferred.get }
          value1   <- cache.put(0, 1)
          _        <- deferred.complete(0)
          value1   <- value1.swap
          value0   <- fiber.join
          value2   <- cache.get(0)
        } yield {
          value0 shouldEqual 0
          value1 shouldEqual 0.some
          value2 shouldEqual 1.some
        }
      }
      result.run()
    }


    test(s"get while getOrUpdate: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, Int]
          value0   <- cache.getOrUpdateEnsure(0) { deferred.get }
          value1   <- Concurrent[IO].startEnsure { cache.get(0) }
          _        <- deferred.complete(0)
          value0   <- value0.join
          value1   <- value1.join
        } yield {
          value0 shouldEqual 0
          value1 shouldEqual 0.some
        }
      }
      result.run()
    }


    test(s"get while getOrUpdate failed: $name") {
      val result = cache.use { cache =>
        for {
          deferred <- Deferred[IO, IO[Int]]
          value0   <- cache.getOrUpdateEnsure(0) { deferred.get.flatten }
          value1   <- Concurrent[IO].startEnsure { cache.get(0) }
          _        <- deferred.complete(TestError.raiseError[IO, Int])
          value0   <- value0.join.attempt
          value1   <- value1.join.attempt
        } yield {
          value0 shouldEqual TestError.asLeft
          value1 shouldEqual TestError.asLeft
        }
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
          value0   <- value0.join
          value1   <- value1.swap
        } yield {
          value0 shouldEqual 0
          value1 shouldEqual 0.some
        }
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
      for {
        deferred <- Deferred[F, Unit]
        fiber    <- Concurrent[F].start {
          self.getOrUpdate(key) {
            for {
              _     <- deferred.complete(())
              value <- value
            } yield value
          }
        }
        _       <- deferred.get
      } yield fiber
    }
  }


  implicit class ConcurrentOps[F[_]](val self: Concurrent[F]) extends AnyVal {

    def startEnsure[A](fa: F[A]): F[Fiber[F, A]] = {
      implicit val F = self
      for {
        started <- Deferred[F, Unit]
        fiber   <- Concurrent[F].start {
          for {
            _ <- started.complete(())
            a <- fa
          } yield a
        }
        _ <- started.get
      } yield fiber
    }
  }




  implicit class CacheSpecOptionFOps[F[_], A](val self: Option[F[A]]) extends AnyVal {

    def swap(implicit F: Monad[F]): F[Option[A]] = {
      self.flatTraverse { _.map(_.some) }
    }
  }
}
