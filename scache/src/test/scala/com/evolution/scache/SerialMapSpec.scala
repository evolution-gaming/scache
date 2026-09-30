package com.evolution.scache

import cats.MonadThrow
import cats.effect.{Async, Concurrent, Deferred, IO, Outcome, Ref}
import cats.kernel.CommutativeMonoid
import cats.syntax.all.*
import com.evolution.scache.IOSuite.*
import com.evolutiongaming.catshelper.CatsHelper.*
import com.evolutiongaming.catshelper.SerialRef
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

class SerialMapSpec extends AsyncFunSuite with Matchers {
  import SerialMapSpec.*

  test("get") {
    get[IO].run()
  }

  test("getOrElse") {
    getOrElse[IO].run()
  }

  test("getOrUpdate") {
    getOrUpdate[IO].run()
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

  test("not lose concurrent update when entry creator fails") {
    `not lose concurrent update when entry creator fails`[IO].run()
  }

  test("not lose concurrent update when modify of existing value fails") {
    `not lose concurrent update when modify of existing value fails`[IO].run()
  }

  test("not lose concurrent put completed before entry creator acquired permit") {
    `not lose concurrent put completed before entry creator acquired permit`.run()
  }

  test("not leak entry when creator is canceled before acquiring permit") {
    `not leak entry when creator is canceled before acquiring permit`.run()
  }

  test("modify serially for the same key") {
    `modify serially for the same key`[IO].run()
  }

  test("modify in parallel for different keys") {
    `modify in parallel for different keys`[IO].run()
  }

  private def get[F[_]: Async] = {
    val key = "key"
    for {
      serialMap <- SerialMap.of[F, String, Int]
      value0 <- serialMap.get(key)
      _ <- serialMap.put(key, 0)
      value1 <- serialMap.get(key)
    } yield {
      value0 shouldEqual none[Int]
      value1 shouldEqual 0.some
    }
  }

  private def getOrElse[F[_]: Async] = {
    val key = "key"
    for {
      serialMap <- SerialMap.of[F, String, Int]
      value0 <- serialMap.getOrElse(key, 1.pure[F])
      _ <- serialMap.put(key, 2)
      value1 <- serialMap.getOrElse(key, 1.pure[F])
    } yield {
      value0 shouldEqual 1
      value1 shouldEqual 2
    }
  }

  private def getOrUpdate[F[_]: Async] = {
    val key = "key"
    for {
      serialMap <- SerialMap.of[F, String, Int]
      started <- Deferred[F, Unit]
      deferred <- Deferred[F, Int]
      value0 <- serialMap.getOrUpdate(key, started.complete(()) *> deferred.get).startEnsure
      _ <- started.get
      value1 <- serialMap.getOrUpdate(key, 1.pure[F]).startEnsure
      _ <- deferred.complete(0)
      value0 <- value0.join
      value1 <- value1.join
    } yield {
      value0 shouldEqual Outcome.succeeded(IO.pure(0))
      value1 shouldEqual Outcome.succeeded(IO.pure(0))
    }
  }

  private def put[F[_]: Async] = {
    val key = "key"
    for {
      serialMap <- SerialMap.of[F, String, Int]
      value0 <- serialMap.put(key, 0)
      value1 <- serialMap.put(key, 1)
      value2 <- serialMap.put(key, 2)
    } yield {
      value0 shouldEqual none[Int]
      value1 shouldEqual 0.some
      value2 shouldEqual 1.some
    }
  }

  private def modify[F[_]: Async] = {
    val key = "key"
    for {
      serialMap <- SerialMap.of[F, String, Int]
      value0 <- serialMap.modify(key) { value =>
        val value1 = value.fold(0) { _ + 1 }
        (value1.some, value).pure[F]
      }
      value1 <- serialMap.modify(key) { value =>
        (none[Int], value).pure[F]
      }
      value2 <- serialMap.modify(key) { value =>
        (none[Int], value).pure[F]
      }
    } yield {
      value0 shouldEqual none[Int]
      value1 shouldEqual 0.some
      value2 shouldEqual none[Int]
    }
  }

  private def update[F[_]: Async] = {
    val key = "key"
    for {
      serialMap <- SerialMap.of[F, String, Int]
      _ <- serialMap.update(key) { _.fold(0) { _ + 1 }.some.pure[F] }
      value0 <- serialMap.get(key)
      _ <- serialMap.update(key) { _ => none[Int].pure[F] }
      value1 <- serialMap.get(key)
      _ <- serialMap.update(key) { _ => none[Int].pure[F] }
      value2 <- serialMap.get(key)
    } yield {
      value0 shouldEqual 0.some
      value1 shouldEqual none[Int]
      value2 shouldEqual none[Int]
    }
  }

  private def size[F[_]: Async] = {
    for {
      serialMap <- SerialMap.of[F, Int, Int]
      size0 <- serialMap.size
      _ <- serialMap.put(0, 0)
      size1 <- serialMap.size
      _ <- serialMap.put(0, 1)
      size2 <- serialMap.size
      _ <- serialMap.put(1, 1)
      size3 <- serialMap.size
      _ <- serialMap.remove(0)
      size4 <- serialMap.size
      _ <- serialMap.clear
      size5 <- serialMap.size
    } yield {
      size0 shouldEqual 0
      size1 shouldEqual 1
      size2 shouldEqual 1
      size3 shouldEqual 2
      size4 shouldEqual 1
      size5 shouldEqual 0
    }
  }

  private def keys[F[_]: Async] = {
    for {
      serialMap <- SerialMap.of[F, Int, Int]
      _ <- serialMap.put(0, 0)
      keys = serialMap.keys
      keys0 <- keys
      _ <- serialMap.put(1, 1)
      keys1 <- keys
      _ <- serialMap.put(2, 2)
      keys2 <- keys
      _ <- serialMap.clear
      keys3 <- keys
    } yield {
      keys0 shouldEqual Set(0)
      keys1 shouldEqual Set(0, 1)
      keys2 shouldEqual Set(0, 1, 2)
      keys3 shouldEqual Set.empty
    }
  }

  private def values[F[_]: Async] = {
    for {
      serialMap <- SerialMap.of[F, Int, Int]
      _ <- serialMap.put(0, 0)
      values0 <- serialMap.values
      _ <- serialMap.put(1, 1)
      values1 <- serialMap.values
      _ <- serialMap.put(2, 2)
      values2 <- serialMap.values
      _ <- serialMap.clear
      values3 <- serialMap.values
    } yield {
      values0 shouldEqual Map((0, 0))
      values1 shouldEqual Map((0, 0), (1, 1))
      values2 shouldEqual Map((0, 0), (1, 1), (2, 2))
      values3 shouldEqual Map.empty
    }
  }

  private def remove[F[_]: Concurrent] = {
    val key = "key"
    val cache = LoadingCache.of(LoadingCache.EntryRefs.empty[F, String, SerialRef[F, SerialMap.State[Int]]])
    cache.use { cache =>
      val serialMap = SerialMap(cache)
      for {
        value0 <- cache.get(key)
        _ <- serialMap.update(key) { _ => 0.some.pure[F] }
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

  private def clear[F[_]: Async] = {
    for {
      serialMap <- SerialMap.of[F, Int, Int]
      _ <- serialMap.put(0, 0)
      _ <- serialMap.put(1, 1)
      _ <- serialMap.clear
      value0 <- serialMap.get(0)
      value1 <- serialMap.get(1)
    } yield {
      value0 shouldEqual none[Int]
      value1 shouldEqual none[Int]
    }
  }

  private def `not leak on failures`[F[_]: Concurrent] = {
    val key = "key"
    val cache = LoadingCache.of(LoadingCache.EntryRefs.empty[F, String, SerialRef[F, SerialMap.State[Int]]])
    cache.use { cache =>
      val serialMap = SerialMap(cache)
      val modifyError = serialMap.modify(key) { _ => TestError.raiseError[F, (Option[Int], Unit)] }.attempt
      for {
        value0 <- modifyError
        value1 <- cache.get(key)
        _ <- serialMap.put(key, 0)
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

  private def `not lose concurrent update when entry creator fails`[F[_]: Async] = {
    val key = "key"
    for {
      serialMap <- SerialMap.of[F, String, Int]
      blocked <- Deferred[F, Unit]
      acquired <- Deferred[F, Unit]
      value0 = serialMap.modify(key) { _ =>
        for {
          _ <- acquired.complete(())
          _ <- blocked.get
          a <- TestError.raiseError[F, (Option[Int], Unit)]
        } yield a
      }
      value0 <- value0.attempt.startEnsure
      _ <- acquired.get
      value1 <- serialMap.put(key, 1).startEnsure
      // make `put` lock on soon to fail `modify` on `value0`
      _ <- Async[F].sleep(100.millis)
      _ <- blocked.complete(())
      value0 <- value0.join
      value1 <- value1.join
      value2 <- serialMap.get(key)
    } yield {
      value0 shouldEqual Outcome.succeeded(IO.pure(TestError.asLeft))
      value1 shouldEqual Outcome.succeeded(IO.pure(none[Int]))
      value2 shouldEqual 1.some
    }
  }

  private def `not lose concurrent update when modify of existing value fails`[F[_]: Async] = {
    val key = "key"
    for {
      serialMap <- SerialMap.of[F, String, Int]
      _ <- serialMap.put(key, 0)
      blocked <- Deferred[F, Unit]
      acquired <- Deferred[F, Unit]
      value0 = serialMap.modify(key) { _ =>
        for {
          _ <- acquired.complete(())
          _ <- blocked.get
          a <- TestError.raiseError[F, (Option[Int], Unit)]
        } yield a
      }
      value0 <- value0.attempt.startEnsure
      _ <- acquired.get
      value1 <- serialMap.put(key, 1).startEnsure
      // make `put` lock on soon to fail `modify` on `value0`
      _ <- Async[F].sleep(100.millis)
      _ <- blocked.complete(())
      value0 <- value0.join
      value1 <- value1.join
      value2 <- serialMap.get(key)
    } yield {
      value0 shouldEqual Outcome.succeeded(IO.pure(TestError.asLeft))
      value1 shouldEqual Outcome.succeeded(IO.pure(0.some))
      value2 shouldEqual 1.some
    }
  }

  private def `not lose concurrent put completed before entry creator acquired permit` = {
    val key = "key"
    Cache.loading[IO, String, SerialRef[IO, SerialMap.State[Int]]].use { cache =>
      for {
        published <- Deferred[IO, Unit]
        proceed <- Deferred[IO, Unit]
        // pause the first caller between: setting value in cache AND it acquires the permit to modify it
        pausing <- onFirstGetOrUpdate(cache) { published.complete(()) *> proceed.get.void }
        serialMap = SerialMap(pausing)
        value0 <- serialMap
          .modify(key) { _ => TestError.raiseError[IO, (Option[Int], Unit)] }
          .attempt
          .start
        _ <- published.get
        value1 <- serialMap.put(key, 1)
        _ <- proceed.complete(())
        value0 <- value0.joinWithNever
        value2 <- serialMap.get(key)
      } yield {
        value0 shouldEqual TestError.asLeft
        value1 shouldEqual none[Int]
        value2 shouldEqual 1.some
      }
    }
  }

  private def `not leak entry when creator is canceled before acquiring permit` = {
    val key = "key"
    Cache.loading[IO, String, SerialRef[IO, SerialMap.State[Int]]].use { cache =>
      for {
        // cancel the first caller right after the entry is set in cache, before it acquires the permit to modify it
        canceling <- onFirstGetOrUpdate(cache) { IO.canceled }
        serialMap = SerialMap(canceling)
        value0 <- serialMap
          .modify(key) { _ => TestError.raiseError[IO, (Option[Int], Unit)] }
          .start
        value0 <- value0.join
        keys <- serialMap.keys
        size <- serialMap.size
      } yield {
        // creator is not interrupted, it proceeds to run `f` and to clean up on its failure
        value0 shouldEqual Outcome.errored[IO, Throwable, Unit](TestError)
        keys shouldEqual Set.empty
        size shouldEqual 0
      }
    }
  }

  private def `modify serially for the same key`[F[_]: Async] = {
    val key = "key"
    for {
      serialMap <- SerialMap.of[F, String, Int]
      blocked <- Deferred[F, Unit]
      acquired <- Deferred[F, Unit]
      value0 = serialMap.modify(key) { value =>
        for {
          _ <- acquired.complete(())
          _ <- blocked.get
        } yield {
          inc(value)
        }
      }
      value0 <- value0.startEnsure
      _ <- acquired.get
      value1 = serialMap.modify(key) { value => inc(value).pure[F] }
      value1 <- value1.startEnsure
      _ <- blocked.complete(())
      value0 <- value0.join
      value1 <- value1.join
    } yield {
      value0 shouldEqual Outcome.succeeded(IO.pure(0))
      value1 shouldEqual Outcome.succeeded(IO.pure(1))
    }
  }

  private def `modify in parallel for different keys`[F[_]: Async] = {
    for {
      serialMap <- SerialMap.of[F, String, Int]
      blocked <- Deferred[F, Unit]
      acquired <- Deferred[F, Unit]
      value0 = serialMap.modify("key1") { value =>
        for {
          _ <- acquired.complete(())
          _ <- blocked.get
        } yield {
          inc(value)
        }
      }
      value0 <- value0.startEnsure
      _ <- acquired.get
      value1 <- serialMap.modify("key2") { v => inc(v).pure[F] }
      _ <- blocked.complete(())
      value0 <- value0.join
    } yield {
      value0 shouldEqual Outcome.succeeded(IO.pure(0))
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

  // run `hook` after the first `getOrUpdate` call returns, after the first entry has been stored in cache
  def onFirstGetOrUpdate[K, V](cache: Cache[IO, K, V])(hook: IO[Unit]): IO[Cache[IO, K, V]] = {
    Ref[IO].of(true).map { first =>
      new DelegatingCache(cache) {
        override def getOrUpdate(key: K)(value: => IO[V]) = {
          super.getOrUpdate(key)(value).flatTap { _ =>
            first.getAndSet(false).flatMap { first => hook.whenA(first) }
          }
        }
      }
    }
  }

  class DelegatingCache[F[_]: MonadThrow, K, V](cache: Cache[F, K, V]) extends Cache.Abstract1[F, K, V] {

    def get(key: K) = cache.get(key)

    def get1(key: K) = cache.get1(key)

    def getOrUpdate(key: K)(value: => F[V]) = cache.getOrUpdate(key)(value)

    def getOrUpdate1[A](key: K)(value: => F[(A, V, Option[Release])]) = cache.getOrUpdate1(key)(value)

    def put(key: K, value: V, release: Option[Release]) = cache.put(key, value, release)

    def modify[A](key: K)(f: Option[V] => (A, Directive[F, V])) = cache.modify(key)(f)

    def contains(key: K) = cache.contains(key)

    def size = cache.size

    def keys = cache.keys

    def values = cache.values

    def values1 = cache.values1

    def remove(key: K) = cache.remove(key)

    def clear: F[Released] = cache.clear

    def foldMap[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = cache.foldMap(f)

    def foldMapPar[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = cache.foldMapPar(f)
  }
}
