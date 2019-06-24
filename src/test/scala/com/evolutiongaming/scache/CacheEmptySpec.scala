package com.evolutiongaming.scache


import cats.effect.concurrent.Deferred
import cats.effect.IO
import cats.implicits._
import com.evolutiongaming.scache.IOSuite._
import org.scalatest.{AsyncFunSuite, Matchers}

class CacheEmptySpec extends AsyncFunSuite with Matchers {
  import CacheSpec._

  private val cache = Cache.empty[IO, Int, Int]

  test("get") {
    val result = for {
      value <- cache.get(0)
    } yield {
      value shouldEqual none[Int]
    }
    result.run()
  }

  test("put") {
    val result = for {
      value0 <- cache.put(0, 0)
      value0 <- value0.swap
      value1 <- cache.get(0)
      value2 <- cache.put(0, 1)
      value2 <- value2.swap
      value3 <- cache.put(0, 2)
      value3 <- value3.swap
    } yield {
      value0 shouldEqual none
      value1 shouldEqual none
      value2 shouldEqual none
      value3 shouldEqual none
    }
    result.run()
  }


  test("remove") {
    val result = for {
      _      <- cache.put(0, 0)
      value0 <- cache.remove(0)
      value0 <- value0.swap
      value1 <- cache.get(0)
    } yield {
      value0 shouldEqual none
      value1 shouldEqual none
    }
    result.run()
  }


  test("clear") {
    val result = for {
      _      <- cache.put(0, 0)
      _      <- cache.put(1, 1)
      _      <- cache.clear
      value0 <- cache.get(0)
      value1 <- cache.get(1)
    } yield {
      value0 shouldEqual none
      value1 shouldEqual none
    }
    result.run()
  }


  test("getOrUpdate") {
    val result = for {
      deferred <- Deferred[IO, Int]
      value0   <- cache.getOrUpdateEnsure(0) { deferred.get }
      value2   <- cache.getOrUpdate(0)(1.pure[IO]).startEnsure
      _        <- deferred.complete(0)
      value0   <- value0.join
      value1   <- value2.join
    } yield {
      value0 shouldEqual 0
      value1 shouldEqual 1
    }
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
