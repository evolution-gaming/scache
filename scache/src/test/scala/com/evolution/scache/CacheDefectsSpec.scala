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
      gate <- Deferred[IO, Unit]
      loader <- cache.getOrUpdate(0) { started.complete(()) *> gate.get.as(1) }.start
      _ <- started.get
      cancelling <- loader.cancel.start
      result <- {
        for {
          cancelled <- cancelling.join.timeout(500.millis)
          _ = cancelled should matchPattern { case Outcome.Succeeded(_) => }
          present <- cache.get(0)
          _ = present shouldEqual none
          second <- cache.getOrUpdate(0)(2.pure[IO]).timeout(500.millis)
          _ = second shouldEqual 2
        } yield ()
      }.guarantee { gate.complete(()) *> cancelling.join.void }
    } yield result
    io.run()
  }

  test("claim 2: expiration cleanup must evict entries stuck in Loading state") {
    val config = ExpiringCache.Config[IO, Int, Int](
      expireAfterRead = 100.millis,
      loadingTimeout = 100.millis.some,
    )
    val io = ExpiringCache.of[IO, Int, Int](config).use { cache =>
      for {
        started <- Deferred[IO, Unit]
        gate <- Deferred[IO, Unit]
        // Attempted, see the test evicting a stuck Loading entry below.
        loader <- cache.getOrUpdate(0) { started.complete(()) *> gate.get.as(1) }.attempt.start
        _ <- started.get
        result <- {
          for {
            _ <- cache.put(1, 1).flatten
            _ <- IO.sleep(500.millis)
            // Control: an ordinary value of the same age is gone, so the cleanup did run.
            control <- cache.contains(1)
            _ = control shouldEqual false
            poisoned <- cache.contains(0)
            _ = poisoned shouldEqual false
            second <- cache.getOrUpdate(0)(2.pure[IO]).timeout(500.millis)
            _ = second shouldEqual 2
          } yield ()
        }.guarantee { gate.complete(()) *> loader.join.void }
      } yield result
    }
    io.run()
  }

  test("claim 3: cancelling a load must unblock the fibers waiting on it") {
    val io = for {
      entryMap <- EntryMap.of[IO, Int, Int]
      cache = LoadingCache(entryMap)
      started <- Deferred[IO, Unit]
      gate <- Deferred[IO, Unit]
      loader <- cache.getOrUpdate(0) { started.complete(()) *> gate.get.as(1) }.start
      _ <- started.get
      // Attempted, so that the failure this test is after is observed as a value: a fiber left to
      // end in `Errored` reports the error to the runtime as unhandled the moment it finishes,
      // which here races with the `join` below.
      waiter <- cache.getOrUpdate(0)(99.pure[IO]).attempt.start
      _ <- IO.sleep(100.millis)
      cancelling <- loader.cancel.start
      result <- {
        for {
          outcome <- waiter.joinWithNever.timeout(500.millis)
          _ = outcome should matchPattern { case Left(CancelledError) => }
          present <- cache.get(0)
          _ = present shouldEqual none
        } yield ()
      }.guarantee { gate.complete(()) *> cancelling.join.void }
    } yield result
    io.run()
  }

  test("claim 3: cancelling a load removed while loading must unblock the fibers waiting on it") {
    val io = for {
      entryMap <- EntryMap.of[IO, Int, Int]
      cache = LoadingCache(entryMap)
      started <- Deferred[IO, Unit]
      gate <- Deferred[IO, Unit]
      loader <- cache.getOrUpdate(0) { started.complete(()) *> gate.get.as(1) }.start
      _ <- started.get
      // Attempted, see the test above.
      waiter <- cache.getOrUpdate(0)(99.pure[IO]).attempt.start
      _ <- IO.sleep(100.millis)
      // The entry stops being the loader's, so only the loader itself can still unblock the waiter.
      _ <- cache.remove(0).flatten
      cancelling <- loader.cancel.start
      result <- {
        for {
          outcome <- waiter.joinWithNever.timeout(500.millis)
          _ = outcome should matchPattern { case Left(CancelledError) => }
        } yield ()
      }.guarantee { gate.complete(()) *> cancelling.join.void }
    } yield result
    io.run()
  }

  test("claim 4: getOrUpdate must complete under sustained writes of unrelated keys") {
    val io = LoadingCache.of[IO, Int, Int].use { cache =>
      for {
        writers <- (1 to 8)
          .toList
          .traverse { key =>
            (cache.put(key, key).flatten *> cache.remove(key).flatten)
              .foreverM
              .start
          }
        _ <- IO.sleep(100.millis)
        result <- {
          for {
            value <- cache.getOrUpdate(0)(1.pure[IO]).timeout(5.seconds)
            _ = value shouldEqual 1
          } yield ()
        }.guarantee { writers.parTraverse_ { _.cancel } }
      } yield result
    }
    io.run(timeout = 30.seconds)
  }

  test("claim 4 mechanism: insert of an unrelated key must not force a retry of getOrUpdate") {
    val io = for {
      underlying <- EntryMap.of[IO, Int, Int]
      attempts <- Ref[IO].of(0)
      noise = insertUnrelated(underlying, 1)
      cache = LoadingCache(intercepted(underlying, noise, attempts.some))
      value <- cache.getOrUpdate(0)(1.pure[IO])
      _ = value shouldEqual 1
      attempts <- attempts.get
      _ = attempts shouldEqual 1
      keys <- cache.keys
      _ = keys shouldEqual Set(0, 1)
    } yield ()
    io.run()
  }

  test("claim 4 mechanism: parallel getOrUpdate of distinct keys causes no insert retries") {
    val io = for {
      underlying <- EntryMap.of[IO, Int, Int]
      attempts <- Ref[IO].of(0)
      cache = LoadingCache(intercepted(underlying, IO.unit, attempts.some))
      _ <- (0 until 10000).toList.parTraverse { key => cache.getOrUpdate(key)(key.pure[IO]) }
      size <- cache.size
      _ = size shouldEqual 10000
      attempts <- attempts.get
      _ = attempts shouldEqual 10000
    } yield ()
    io.run(timeout = 30.seconds)
  }

  test("evicting a stuck Loading entry unblocks fibers waiting on it") {
    val config = ExpiringCache.Config[IO, Int, Int](
      expireAfterRead = 1.minute,
      loadingTimeout = 100.millis.some,
    )
    val io = ExpiringCache.of[IO, Int, Int](config).use { cache =>
      for {
        started <- Deferred[IO, Unit]
        gate <- Deferred[IO, Unit]
        // Attempted, because the eviction makes this load fail too, and a fiber left to end in
        // `Errored` reports the error to the runtime as unhandled the moment it finishes, before
        // the `join` below gets to observe it.
        loader <- cache.getOrUpdate(0) { started.complete(()) *> gate.get.as(1) }.attempt.start
        _ <- started.get
        waiter <- cache.getOrUpdate(0)(99.pure[IO]).attempt.start
        result <- {
          for {
            outcome <- waiter.joinWithNever.timeout(2.seconds)
            _ = outcome should matchPattern { case Left(ExpiredError) => }
            _ <- gate.complete(())
            // The fiber whose load was evicted learns about it as well.
            evicted <- loader.joinWithNever.timeout(2.seconds)
            _ = evicted should matchPattern { case Left(ExpiredError) => }
          } yield ()
        }.guarantee { gate.complete(()).attempt *> loader.join.void }
      } yield result
    }
    io.run()
  }

  test("a new load generation does not inherit the previous generation's stuck-timer") {
    val config = ExpiringCache.Config[IO, Int, Int](
      expireAfterRead = 1.minute,
      loadingTimeout = 200.millis.some,
    )
    val io = ExpiringCache.of[IO, Int, Int](config).use { cache =>
      for {
        started1 <- Deferred[IO, Unit]
        gate1 <- Deferred[IO, Unit]
        loader1 <- cache.getOrUpdate(0) { started1.complete(()) *> gate1.get.as(1) }.start
        _ <- started1.get
        _ <- IO.sleep(150.millis)
        _ <- gate1.complete(())
        _ <- loader1.join
        _ <- cache.remove(0).flatten
        started2 <- Deferred[IO, Unit]
        gate2 <- Deferred[IO, Unit]
        loader2 <- cache.getOrUpdate(0) { started2.complete(()) *> gate2.get.as(2) }.start
        _ <- started2.get
        result <- {
          for {
            _ <- IO.sleep(150.millis)
            present <- cache.contains(0)
            _ = present shouldEqual true
          } yield ()
        }.guarantee { gate2.complete(()) *> loader2.join.void }
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
          // The key must be usable right away, holding either the value of the load that made it
          // in before the cancellation, or the one we compute here.
          value <- cache.getOrUpdate(0)((-1).pure[IO]).timeout(1.second)
          _ = value should (equal(i) or equal(-1))
          _ <- cache.remove(0).flatten
        } yield ()
      }
      // Releases of values nobody asked about are started in the background, so the balance is
      // settled shortly after the last removal rather than at the moment of it.
      _ <- (IO.sleep(10.millis) *> balance.get).iterateUntil { _ == 0 }.timeout(3.seconds)
    } yield ()
    io.run(timeout = 60.seconds)
  }

  test("modify racing remove over a loading key neither leaks nor double-releases") {
    val io = for {
      entryMap <- EntryMap.of[IO, Int, Int]
      cache = LoadingCache(entryMap)
      balance <- Ref[IO].of(0)
      _ <- (1 to 500).toList.traverse_ { i =>
        for {
          started <- Deferred[IO, Unit]
          gate <- Deferred[IO, Unit]
          loader <- cache.getOrUpdate(0) { started.complete(()) *> gate.get.as(-i) }.start
          _ <- started.get
          // The waiters widen the window between modify completing the deferred and committing the
          // entry, which is exactly where the concurrent remove has to land.
          waiters <- (1 to 8).toList.traverse { _ => cache.getOrUpdate(0)((-i).pure[IO]).start }
          _ <- balance.update { _ + 1 }
          _ <- (
            cache.modify(0) { _ => ((), Cache.Directive.Put(i, balance.update { _ - 1 }.some)) },
            cache.remove(0).flatten,
          ).parTupled
          _ <- gate.complete(())
          _ <- loader.join
          _ <- waiters.traverse_ { _.join }
          _ <- cache.remove(0).flatten
        } yield ()
      }
      // Exactly one release per iteration must run: the balance ends below zero on a double
      // release and above zero on a leak, and either keeps this from ever reaching zero.
      _ <- (IO.sleep(10.millis) *> balance.get).iterateUntil { _ == 0 }.timeout(3.seconds)
    } yield ()
    io.run(timeout = 60.seconds)
  }

  test("modify must release the value it published when its commit loses to remove") {
    val io = for {
      entryMap <- EntryMap.of[IO, Int, Int]
      cache = LoadingCache(entryMap)
      balance <- Ref[IO].of(0)
      deferred <- Deferred[IO, Either[Throwable, LoadingCache.Entry[IO, Int]]]
      inner <- Ref[IO].of[LoadingCache.EntryState[IO, Int]](LoadingCache.EntryState.Loading(deferred))
      armed <- Ref[IO].of(true)
      // Fired by the commit attempt of `modify`, i.e. between it completing the deferred and
      // writing the entry: the remove steals the entry, the put takes the key over.
      noise = armed.getAndSet(false).flatMap {
        case true => cache.remove(0).flatten *> cache.put(0, 99).flatten.void
        case false => IO.unit
      }
      _ <- entryMap.ref(0).set(interceptedCommit(inner, noise).some)
      _ <- balance.update { _ + 1 }
      _ <- cache.modify(0) {
        case None => ((), Cache.Directive.Put(1, balance.update { _ - 1 }.some))
        case Some(_) => ((), Cache.Directive.Ignore)
      }
      published <- deferred.get.timeout(1.second)
      _ = published.map { _.value } shouldEqual 1.asRight
      // The key must be held by the value the injected put installed, which is what proves the
      // remove-put pair did land between the deferred completion and the commit.
      taken <- cache.get(0)
      _ = taken shouldEqual 99.some
      _ <- cache.remove(0).flatten
      _ <- (IO.sleep(10.millis) *> balance.get).iterateUntil { _ == 0 }.timeout(3.seconds)
    } yield ()
    io.run()
  }

  /**
   * An entry `Ref` whose `access` setter runs `noise` before committing, so that a test can inject
   * a concurrent state transition exactly between an operation reading the entry state and writing
   * it back. Every other method, `getAndSet` of `remove` included, goes to `inner` untouched.
   */
  private def interceptedCommit(
    inner: Ref[IO, LoadingCache.EntryState[IO, Int]],
    noise: IO[Unit],
  ): EntryRef[IO, Int] = {
    type A = LoadingCache.EntryState[IO, Int]
    new Ref[IO, A] {
      def get: IO[A] = inner.get
      def set(a: A): IO[Unit] = inner.set(a)
      def access: IO[(A, A => IO[Boolean])] = inner.access.map { case (a, set) => (a, (a1: A) => noise *> set(a1)) }
      def tryUpdate(f: A => A): IO[Boolean] = inner.tryUpdate(f)
      def tryModify[B](f: A => (A, B)): IO[Option[B]] = inner.tryModify(f)
      def update(f: A => A): IO[Unit] = inner.update(f)
      def modify[B](f: A => (A, B)): IO[B] = inner.modify(f)
      def tryModifyState[B](state: cats.data.State[A, B]): IO[Option[B]] = inner.tryModifyState(state)
      def modifyState[B](state: cats.data.State[A, B]): IO[B] = inner.modifyState(state)
    }
  }

  private def insertUnrelated(underlying: EntryMap[IO, Int, Int], key: Int): IO[Unit] = {
    for {
      entryRef <- Ref[IO].of[LoadingCache.EntryState[IO, Int]](
        LoadingCache.EntryState.Value(LoadingCache.Entry(key, none)),
      )
      _ <- underlying.ref(key).set(entryRef.some)
    } yield ()
  }

  /**
   * `underlying` with every per-key `Ref` wrapped, so that each attempt of the cache to modify the
   * mapping first runs `noise`, a write of some other key, and then is counted in `attempts`.
   *
   * That gives the deterministic version of what the `claim 4` test does with background fibers: an
   * unrelated write is guaranteed to land between reading and writing the mapping, i.e. exactly
   * where the shared `Ref[F, Map[K, EntryRef]]` used to lose its CAS. With a per-key `Ref` the
   * attempt still succeeds, so the count stays at one attempt per insert.
   *
   * `noise` writes through `underlying` directly and is not counted.
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
