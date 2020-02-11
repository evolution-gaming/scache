package com.evolutiongaming.scache

import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, IO}
import cats.implicits._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.SerialRef
import com.evolutiongaming.scache.IOSuite._

import scala.util.control.NoStackTrace
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

class SerialMapSpec extends AsyncFunSuite with Matchers {
  import SerialMapSpec._

  test("get") {
    get[IO].run()
  }

  test("put") {
    put[IO].run()
  }

  test("modify") {
    modify[IO].run()
  }

  test("update") {
    update[IO].run()
  }

  test("size") {
    size[IO].run()
  }

  test("keys") {
    keys[IO].run()
  }

  test("values") {
    values[IO].run()
  }

  test("remove") {
    remove[IO].run()
  }

  test("clear") {
    clear[IO].run()
  }

  test("not leak on failures") {
    `not leak on failures`[IO].run()
  }

  test("modify serially for the same key") {
    `modify serially for the same key`[IO].run()
  }

  test("modify in parallel for different keys") {
    `modify in parallel for different keys`[IO].run()
  }

  private def get[F[_] : Concurrent] = {
    val key = "key"
    for {
      serialMap <- SerialMap.of[F, String, Int]
      value0    <- serialMap.get(key)
      _         <- serialMap.put(key, 0)
      value1    <- serialMap.get(key)
    } yield {
      value0 shouldEqual none[Int]
      value1 shouldEqual 0.some
    }
  }

  private def put[F[_] : Concurrent] = {
    val key = "key"
    for {
      serialMap <- SerialMap.of[F, String, Int]
      value0    <- serialMap.put(key, 0)
      value1    <- serialMap.put(key, 1)
      value2    <- serialMap.put(key, 2)
    } yield {
      value0 shouldEqual none[Int]
      value1 shouldEqual 0.some
      value2 shouldEqual 1.some
    }
  }

  private def modify[F[_] : Concurrent] = {
    val key = "key"
    for {
      serialMap <- SerialMap.of[F, String, Int]
      value0    <- serialMap.modify(key) { value =>
        val value1 = value.fold(0) { _ + 1 }
        (value1.some, value).pure[F]
      }
      value1    <- serialMap.modify(key) { value =>
        (none[Int], value).pure[F]
      }
      value2    <- serialMap.modify(key) { value =>
        (none[Int], value).pure[F]
      }
    } yield {
      value0 shouldEqual none[Int]
      value1 shouldEqual 0.some
      value2 shouldEqual none[Int]
    }
  }


  private def update[F[_] : Concurrent] = {
    val key = "key"
    for {
      serialMap <- SerialMap.of[F, String, Int]
      _         <- serialMap.update(key) { _.fold(0) { _ + 1 }.some.pure[F] }
      value0    <- serialMap.get(key)
      _         <- serialMap.update(key) { _ => none[Int].pure[F] }
      value1    <- serialMap.get(key)
      _         <- serialMap.update(key) { _ => none[Int].pure[F] }
      value2    <- serialMap.get(key)
    } yield {
      value0 shouldEqual 0.some
      value1 shouldEqual none[Int]
      value2 shouldEqual none[Int]
    }
  }


  private def size[F[_] : Concurrent] = {
    for {
      serialMap <- SerialMap.of[F, Int, Int]
      size0     <- serialMap.size
      _         <- serialMap.put(0, 0)
      size1     <- serialMap.size
      _         <- serialMap.put(0, 1)
      size2     <- serialMap.size
      _         <- serialMap.put(1, 1)
      size3     <- serialMap.size
      _         <- serialMap.remove(0)
      size4     <- serialMap.size
      _         <- serialMap.clear
      size5     <- serialMap.size
    } yield {
      size0 shouldEqual 0
      size1 shouldEqual 1
      size2 shouldEqual 1
      size3 shouldEqual 2
      size4 shouldEqual 1
      size5 shouldEqual 0
    }
  }


  private def keys[F[_] : Concurrent] = {
    for {
      serialMap <- SerialMap.of[F, Int, Int]
      _         <- serialMap.put(0, 0)
      keys       = serialMap.keys
      keys0     <- keys
      _         <- serialMap.put(1, 1)
      keys1     <- keys
      _         <- serialMap.put(2, 2)
      keys2     <- keys
      _         <- serialMap.clear
      keys3     <- keys
    } yield {
      keys0 shouldEqual Set(0)
      keys1 shouldEqual Set(0, 1)
      keys2 shouldEqual Set(0, 1, 2)
      keys3 shouldEqual Set.empty
    }
  }


  private def values[F[_] : Concurrent] = {
    for {
      serialMap <- SerialMap.of[F, Int, Int]
      _         <- serialMap.put(0, 0)
      values0   <- serialMap.values
      _         <- serialMap.put(1, 1)
      values1   <- serialMap.values
      _         <- serialMap.put(2, 2)
      values2   <- serialMap.values
      _         <- serialMap.clear
      values3   <- serialMap.values
    } yield {
      values0 shouldEqual Map((0, 0))
      values1 shouldEqual Map((0, 0), (1, 1))
      values2 shouldEqual Map((0, 0), (1, 1), (2, 2))
      values3 shouldEqual Map.empty
    }
  }


  private def remove[F[_] : Concurrent] = {
    val key = "key"
    val cache = LoadingCache.of(LoadingCache.EntryRefs.empty[F, String, SerialRef[F, SerialMap.State[Int]]])
    cache.use { cache =>
      val serialMap = SerialMap(cache)
      for {
        value0 <- cache.get(key)
        _      <- serialMap.update(key) { _ => 0.some.pure[F] }
        value1 <- cache.get(key)
        value2 <- serialMap.remove(key)
        value3 <- cache.get(key)
      } yield {
        value0.isDefined shouldEqual false
        value1.isDefined shouldEqual true
        value2 shouldEqual 0.some
        value3.isDefined shouldEqual false
      }
    }
  }


  private def clear[F[_] : Concurrent] = {
    for {
      serialMap <- SerialMap.of[F, Int, Int]
      _         <- serialMap.put(0, 0)
      _         <- serialMap.put(1, 1)
      _         <- serialMap.clear
      value0    <- serialMap.get(0)
      value1    <- serialMap.get(1)
    } yield {
      value0 shouldEqual none[Int]
      value1 shouldEqual none[Int]
    }
  }


  private def `not leak on failures`[F[_] : Concurrent] = {
    val key = "key"
    val cache = LoadingCache.of(LoadingCache.EntryRefs.empty[F, String, SerialRef[F, SerialMap.State[Int]]])
    cache.use { cache =>
      val serialMap = SerialMap(cache)
      val modifyError = serialMap.modify(key) { _ => TestError.raiseError[F, (Option[Int], Unit)] }.attempt
      for {
        value0 <- modifyError
        value1 <- cache.get(key)
        _      <- serialMap.put(key, 0)
        value2 <- modifyError
        value3 <- serialMap.get(key)
      } yield {
        value0 shouldEqual TestError.asLeft
        value1.isDefined shouldEqual false
        value2 shouldEqual TestError.asLeft
        value3 shouldEqual 0.some
      }
    }
  }


  private def `modify serially for the same key`[F[_] : Concurrent] = {
    val key = "key"
    for {
      serialMap <- SerialMap.of[F, String, Int]
      blocked   <- Deferred[F, Unit]
      acquired  <- Deferred[F, Unit]
      value0     = serialMap.modify(key) { value =>
        for {
          _ <- acquired.complete(())
          _ <- blocked.get
        } yield {
          inc(value)
        }
      }
      value0    <- value0.startEnsure
      _         <- acquired.get
      value1     = serialMap.modify(key) { value => inc(value).pure[F] }
      value1    <- value1.startEnsure
      _         <- blocked.complete(())
      value0    <- value0.join
      value1    <- value1.join
    } yield {
      value0 shouldEqual 0
      value1 shouldEqual 1
    }
  }


  private def `modify in parallel for different keys`[F[_] : Concurrent] = {
    for {
      serialMap <- SerialMap.of[F, String, Int]
      blocked   <- Deferred[F, Unit]
      acquired  <- Deferred[F, Unit]
      value0     = serialMap.modify("key1") { value =>
        for {
          _ <- acquired.complete(())
          _ <- blocked.get
        } yield {
          inc(value)
        }
      }
      value0    <- value0.startEnsure
      _         <- acquired.get
      value1    <- serialMap.modify("key2") { v => inc(v).pure[F] }
      _         <- blocked.complete(())
      value0    <- value0.join
    } yield {
      value0 shouldEqual 0
      value1 shouldEqual 0
    }
  }
}

object SerialMapSpec {

  def inc(value: Option[Int]): (Option[Int], Int) = {
    val value1 = value.fold(0) { _ + 1 }
    (value1.some, value1)
  }

  case object TestError extends RuntimeException with NoStackTrace
}
