package com.evolution.scache

import cats.effect.*
import cats.syntax.all.*
import com.evolution.scache.IOSuite.*
import com.evolution.scache.LoadingCache.{EntryMap, EntryRef}
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

/**
 * Asserts the expected behavior for four defects originally present in LoadingCache /
 * ExpiringCache, fixed by rebuilding the cache on [[cats.effect.std.MapRef]]:
 *   - claim 1: loads are cancelable and cancellation cleans up the `Loading` entry;
 *   - claim 2: entries stuck in `Loading` state are evicted by the expiration routine;
 *   - claim 3: waiters on a `Loading` entry are unblocked when the load is cancelled;
 *   - claim 4: operations on distinct keys are independent, no shared-state CAS retries.
 *
 * Every stuck load is modeled with a `gate` Deferred instead of `IO.never` and released in a
 * `guarantee`, so a failed assertion produces a clean test failure instead of hanging resource
 * finalizers (`clear` waits on Loading entries).
 */
class CacheDefectsSpec extends AsyncFunSuite with Matchers {

  test("claim 1: cancelled load must not block the key for subsequent calls") {
    val io = for {
      entryMap <- EntryMap.of[IO, Int, Int]
      cache = LoadingCache(entryMap)
      started <- Deferred[IO, Unit]
      gate <- Deferred[IO, Int]
      loader <- cache.getOrUpdate(0) { started.complete(()) *> gate.get }.start
      _ <- started.get
      cancelling <- loader.cancel.start
      result <- {
        for {
          cancelled <- cancelling.join.timeout(500.millis).attempt
          _ <- IO { cancelled should matchPattern { case Right(_) => } }
          second <- cache.getOrUpdate(0)(1.pure[IO]).timeout(500.millis).attempt
          _ <- IO { second shouldEqual 1.asRight }
        } yield {}
      }.guarantee { gate.complete(42) *> cancelling.join.void }
    } yield result
    io.run()
  }

  test("claim 2: expiration cleanup must evict entries stuck in Loading state") {
    val config = ExpiringCache.Config[IO, Int, Int](expireAfterRead = 100.millis)
    val io = ExpiringCache.of[IO, Int, Int](config).use { cache =>
      for {
        started <- Deferred[IO, Unit]
        gate <- Deferred[IO, Int]
        loader <- cache.getOrUpdate(0) { started.complete(()) *> gate.get }.start
        _ <- started.get
        result <- {
          for {
            _ <- cache.put(1, 1).flatten
            _ <- IO.sleep(500.millis)
            control <- cache.contains(1)
            _ <- IO { control shouldEqual false }
            poisoned <- cache.contains(0)
            _ <- IO { poisoned shouldEqual false }
            second <- cache.getOrUpdate(0)(2.pure[IO]).timeout(500.millis).attempt
            _ <- IO { second shouldEqual 2.asRight }
          } yield {}
        }.guarantee { gate.complete(42) *> loader.join.void }
      } yield result
    }
    io.run()
  }

  test("claim 3: remove must unblock fibers waiting on a Loading entry") {
    val io = for {
      entryMap <- EntryMap.of[IO, Int, Int]
      cache = LoadingCache(entryMap)
      started <- Deferred[IO, Unit]
      gate <- Deferred[IO, Int]
      loader <- cache.getOrUpdate(0) { started.complete(()) *> gate.get }.start
      _ <- started.get
      waiter <- cache.getOrUpdate(0)(99.pure[IO]).start
      _ <- IO.sleep(100.millis)
      cancelling <- loader.cancel.start
      _ <- IO.sleep(100.millis)
      _ <- cache.remove(0).flatten
      result <- {
        for {
          outcome <- waiter.join.timeout(500.millis).attempt
          _ <- IO { outcome should matchPattern { case Right(_) => } }
        } yield {}
      }.guarantee { gate.complete(42) *> cancelling.join.void }
    } yield result
    io.run()
  }

  test("claim 4: getOrUpdate must not fail due to sustained writes of unrelated keys") {
    val io = for {
      underlying <- EntryMap.of[IO, Int, Int]
      counter <- Ref[IO].of(0)
      noise = counter
        .updateAndGet { _ + 1 }
        .flatMap { key => insertUnrelated(underlying, key) }
      cache = LoadingCache(intercepted(underlying, noise, none))
      result <- cache.getOrUpdate(0)(1.pure[IO]).timeout(10.seconds).attempt
      _ <- IO { result shouldEqual 1.asRight }
    } yield {}
    io.run()
  }

  test("claim 4 mechanism: insert of an unrelated key must not force a retry of getOrUpdate") {
    val io = for {
      underlying <- EntryMap.of[IO, Int, Int]
      attempts <- Ref[IO].of(0)
      noise = insertUnrelated(underlying, 1)
      cache = LoadingCache(intercepted(underlying, noise, attempts.some))
      value <- cache.getOrUpdate(0)(1.pure[IO])
      _ <- IO { value shouldEqual 1 }
      attempts <- attempts.get
      _ <- IO { attempts shouldEqual 1 }
      keys <- cache.keys
      _ <- IO { keys shouldEqual Set(0, 1) }
    } yield {}
    io.run()
  }

  test("claim 4 mechanism: parallel getOrUpdate of distinct keys causes no insert retries") {
    val io = for {
      underlying <- EntryMap.of[IO, Int, Int]
      attempts <- Ref[IO].of(0)
      cache = LoadingCache(intercepted(underlying, IO.unit, attempts.some))
      _ <- (0 until 10000).toList.parTraverse { key => cache.getOrUpdate(key)(key.pure[IO]) }
      size <- cache.size
      _ <- IO { size shouldEqual 10000 }
      attempts <- attempts.get
      _ <- IO { attempts shouldEqual 10000 }
    } yield {}
    io.run(timeout = 30.seconds)
  }

  test("evicting a stuck Loading entry unblocks fibers waiting on it") {
    val config = ExpiringCache.Config[IO, Int, Int](expireAfterRead = 100.millis)
    val io = ExpiringCache.of[IO, Int, Int](config).use { cache =>
      for {
        started <- Deferred[IO, Unit]
        gate <- Deferred[IO, Int]
        loader <- cache.getOrUpdate(0) { started.complete(()) *> gate.get }.start
        _ <- started.get
        waiter <- cache.getOrUpdate(0)(99.pure[IO]).attempt.start
        result <- {
          for {
            outcome <- waiter.joinWithNever.timeout(2.seconds)
            _ <- IO { outcome should matchPattern { case Left(ExpiredError) => } }
          } yield {}
        }.guarantee { gate.complete(42) *> loader.join.void }
      } yield result
    }
    io.run()
  }

  test("a new load generation does not inherit the previous generation's stuck-timer") {
    val config = ExpiringCache.Config[IO, Int, Int](expireAfterRead = 200.millis)
    val io = ExpiringCache.of[IO, Int, Int](config).use { cache =>
      for {
        started1 <- Deferred[IO, Unit]
        gate1 <- Deferred[IO, Int]
        loader1 <- cache.getOrUpdate(0) { started1.complete(()) *> gate1.get }.start
        _ <- started1.get
        _ <- IO.sleep(150.millis)
        _ <- gate1.complete(1)
        _ <- loader1.join
        _ <- cache.remove(0).flatten
        started2 <- Deferred[IO, Unit]
        gate2 <- Deferred[IO, Int]
        loader2 <- cache.getOrUpdate(0) { started2.complete(()) *> gate2.get }.start
        _ <- started2.get
        result <- {
          for {
            _ <- IO.sleep(150.millis)
            present <- cache.contains(0)
            _ <- IO { present shouldEqual true }
          } yield {}
        }.guarantee { gate2.complete(2) *> loader2.join.void }
      } yield result
    }
    io.run()
  }

  test("cancellation races neither poison the key nor leak releases") {
    val io = for {
      entryMap <- EntryMap.of[IO, Int, Int]
      cache = LoadingCache(entryMap)
      balance <- Ref[IO].of(0)
      _ <- (1 to 500).toList.traverse_ { i =>
        for {
          fiber <- cache.getOrUpdate1(0) { balance.update { _ + 1 }.as((i, i, balance.update { _ - 1 }.some)) }.start
          _ <- fiber.cancel.start
          _ <- fiber.join
          _ <- cache.getOrUpdate(0)((-1).pure[IO]).timeout(1.second)
          _ <- cache.remove(0).flatten
        } yield {}
      }
      _ <- (IO.sleep(10.millis) *> balance.get).iterateUntil { _ == 0 }.timeout(3.seconds)
    } yield {}
    io.run(timeout = 60.seconds)
  }

  private def insertUnrelated(underlying: EntryMap[IO, Int, Int], key: Int): IO[Unit] = {
    for {
      entryRef <- Ref[IO].of[LoadingCache.EntryState[IO, Int]](
        LoadingCache.EntryState.Value(LoadingCache.Entry(key, none)),
      )
      _ <- underlying.ref(key).set(entryRef.some)
    } yield {}
  }

  /**
   * EntryMap that runs `noise` before every entry transition going through the cache (simulating a
   * concurrent writer of other keys) and counts those transitions, so the tests can assert that
   * writes to unrelated keys neither invalidate the transition nor force retries. `noise` writes
   * through `underlying` directly and is not counted.
   */
  private def intercepted(
    underlying: EntryMap[IO, Int, Int],
    noise: IO[Unit],
    attempts: Option[Ref[IO, Int]],
  ): EntryMap[IO, Int, Int] = {
    def wrap(ref: Ref[IO, Option[EntryRef[IO, Int]]]): Ref[IO, Option[EntryRef[IO, Int]]] = {
      type A = Option[EntryRef[IO, Int]]
      val observe = noise *> attempts.foldMapM { _.update { _ + 1 } }
      new Ref[IO, A] {
        def get: IO[A] = ref.get
        def set(a: A): IO[Unit] = observe *> ref.set(a)
        def access: IO[(A, A => IO[Boolean])] = ref.access.map { case (a, set) => (a, (a1: A) => observe *> set(a1)) }
        def tryUpdate(f: A => A): IO[Boolean] = observe *> ref.tryUpdate(f)
        def tryModify[B](f: A => (A, B)): IO[Option[B]] = observe *> ref.tryModify(f)
        def update(f: A => A): IO[Unit] = observe *> ref.update(f)
        def modify[B](f: A => (A, B)): IO[B] = observe *> ref.modify(f)
        def tryModifyState[B](state: cats.data.State[A, B]): IO[Option[B]] = observe *> ref.tryModifyState(state)
        def modifyState[B](state: cats.data.State[A, B]): IO[B] = observe *> ref.modifyState(state)
      }
    }

    new EntryMap[IO, Int, Int] {
      def ref(key: Int): Ref[IO, Option[EntryRef[IO, Int]]] = wrap(underlying.ref(key))
      def lookup(key: Int): IO[Option[EntryRef[IO, Int]]] = underlying.lookup(key)
      def keys: IO[Set[Int]] = underlying.keys
      def entries: IO[List[(Int, EntryRef[IO, Int])]] = underlying.entries
      def size: IO[Int] = underlying.size
      def contains(key: Int): IO[Boolean] = underlying.contains(key)
    }
  }
}
