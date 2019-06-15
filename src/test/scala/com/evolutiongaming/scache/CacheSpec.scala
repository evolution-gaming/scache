package com.evolutiongaming.scache

import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, IO}
import cats.implicits._
import com.evolutiongaming.scache.IOSuite._
import org.scalatest.{AsyncFunSuite, Matchers}

import scala.util.control.NoStackTrace

class CacheSpec extends AsyncFunSuite with Matchers {

  for {
    (name, cache) <- List(
      ("default"           , Cache.of[IO, Int, Int]),
      ("nrOfPartitions: 1" , Cache.of[IO, Int, Int](nrOfPartitions = 1)),
      ("nrOfPartitions: 2" , Cache.of[IO, Int, Int](nrOfPartitions = 2)),
      ("without partitions", Cache.of[IO, Int, Int](map = Map.empty[Int, IO[Int]])))
  } yield {

    test(s"get $name") {
      val result = for {
        cache <- cache
        value <- cache.get(0)
      } yield {
        value shouldEqual none[Int]
      }
      result.run()
    }

    test(s"put $name") {
      val result = for {
        cache <- cache
        prev  <- cache.put(0, 0)
        value <- cache.get(0)
      } yield {
        prev shouldEqual none[Int]
        value  shouldEqual 0.some
      }
      result.run()
    }


    test(s"put & get many $name") {

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

      val result = for {
        cache   <- cache
        result0 <- getAll(cache)
        _       <- values.foldMapM { key => cache.put(key, key).void }
        result1 <- getAll(cache)
      } yield {
        result0.flatten.reverse shouldEqual List.empty
        result1.flatten.reverse shouldEqual values
      }
      result.run()
    }


    test(s"getOrUpdate: $name") {

      val result = for {
        cache     <- cache
        deferred0 <- Deferred[IO, Unit]
        deferred1 <- Deferred[IO, Unit]
        update0   <- Concurrent[IO].start {
          cache.getOrUpdate(0) {
            for {
              _ <- deferred0.complete(())
              _ <- deferred1.get
            } yield 0
          }
        }
        _         <- deferred0.get
        update1   <- Concurrent[IO].start { cache.getOrUpdate(0)(1.pure[IO]) }
        _         <- deferred1.complete(())
        update0   <- update0.join
        update1   <- update1.join
      } yield {
        update0 shouldEqual 0
        update1 shouldEqual 0
      }
      result.run()
    }


    test(s"cancellation: $name") {
      val result = for {
        cache     <- cache
        deferred0 <- Deferred[IO, Unit]
        deferred1 <- Deferred[IO, Int]
        fiber     <- Concurrent[IO].start {
          cache.getOrUpdate(0) {
            for {
              _ <- deferred0.complete(())
              a <- deferred1.get
            } yield a
          }
        }
        _         <- deferred0.get
        _         <- fiber.cancel
        _         <- deferred1.complete(0)
        value     <- cache.get(0)
      } yield {
        value shouldEqual 0.some
      }
      result.run()
    }
    

    test(s"no leak in case of failure $name") {
      val result = for {
        cache   <- cache
        result0 <- cache.getOrUpdate(0)(TestError.raiseError[IO, Int]).attempt
        result1 <- cache.getOrUpdate(0)(0.pure[IO]).attempt
        result2 <- cache.getOrUpdate(0)(TestError.raiseError[IO, Int]).attempt
      } yield {
        result0 shouldEqual TestError.asLeft[Int]
        result1 shouldEqual 0.asRight
        result2 shouldEqual 0.asRight
      }
      result.run()
    }
  }

  case object TestError extends RuntimeException with NoStackTrace
}
