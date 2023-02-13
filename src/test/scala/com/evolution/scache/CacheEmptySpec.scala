package com.evolution.scache

import cats.effect.{Deferred, IO, Sync}
import cats.syntax.all.*
import com.evolution.scache.IOSuite.*
import com.evolutiongaming.catshelper.CatsHelper.*
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

class CacheEmptySpec extends AsyncFunSuite with Matchers {
  import CacheSpec.*

  private val cache = Cache.empty[IO, Int, Int]

  test("get") {
    val result = for {
      value <- cache.get(0)
      _     <- Sync[IO].delay { value shouldEqual none[Int] }
    } yield {}
    result.run()
  }

  test("getOrElse1") {
    val result = for {
      value <- cache.getOrElse1(0, 1.pure[IO])
      _     <- Sync[IO].delay { value shouldEqual 1 }
      _     <- cache.put(0, 2)
      value <- cache.getOrElse1(0, 1.pure[IO])
      _     <- Sync[IO].delay { value shouldEqual 1 }
    } yield {}
    result.run()
  }

  test("put") {
    val result = for {
      value <- cache.put(0, 0)
      value <- value
      _     <- Sync[IO].delay { value shouldEqual none }
      value <- cache.get(0)
      _     <- Sync[IO].delay { value shouldEqual none }
      value <- cache.put(0, 1)
      value <- value
      _     <- Sync[IO].delay { value shouldEqual none }
      value <- cache.put(0, 2)
      value <- value
      _     <- Sync[IO].delay { value shouldEqual none }
    } yield {}
    result.run()
  }


  test("remove") {
    val result = for {
      _     <- cache.put(0, 0)
      value <- cache.remove(0)
      value <- value
      _     <- Sync[IO].delay { value shouldEqual none }
      value <- cache.get(0)
      _     <- Sync[IO].delay { value shouldEqual none }
    } yield {}
    result.run()
  }


  test("clear") {
    val result = for {
      _     <- cache.put(0, 0)
      _     <- cache.put(1, 1)
      _     <- cache.clear
      value <- cache.get(0)
      _     <- Sync[IO].delay { value shouldEqual none }
      value <- cache.get(1)
      _     <- Sync[IO].delay { value shouldEqual none }
    } yield {}
    result.run()
  }


  test("getOrUpdate") {
    val result = for {
      deferred <- Deferred[IO, Int]
      value0   <- cache.getOrUpdateEnsure(0) { deferred.get }
      value2   <- cache.getOrUpdate(0)(1.pure[IO]).startEnsure
      _        <- deferred.complete(0)
      value0   <- value0.joinWithNever
      value1   <- value2.joinWithNever
      _        <- IO { value0 shouldEqual 0.asRight }
      _        <- IO { value1 shouldEqual 1 }
    } yield {}
    result.run()
  }

  test("keys") {
    val result = for {
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
      keys0 shouldEqual Set.empty
      keys1 shouldEqual Set.empty
      keys2 shouldEqual Set.empty
      keys3 shouldEqual Set.empty
    }

    result.run()
  }


  test("values") {
    val result = for {
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
      values0 shouldEqual Map.empty
      values1 shouldEqual Map.empty
      values2 shouldEqual Map.empty
      values3 shouldEqual Map.empty
    }
    result.run()
  }
}
