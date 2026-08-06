package com.evolution.scache

import cats.effect.*
import cats.syntax.all.*
import com.evolution.scache.IOSuite.*
import com.evolution.scache.LoadingCache.EntryRefs
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.TimeoutException
import scala.concurrent.duration.*

/**
 * Demonstrates the four defects of the cache as it stands on `master`, i.e. before it is rebuilt on
 * [[cats.effect.std.MapRef]].
 *
 * Each test asserts and prints what the cache actually does today, so the suite is green here, and
 * says in a comment what the same scenario does after the rewrite, where the assertions are
 * inverted. The counterpart suite is `CacheDefectsSpec` of the rewrite branch.
 *
 * Everything that may hang is run in a fiber and awaited with a timeout on the `join`, never with a
 * timeout on the operation itself: the operations below block inside an uncancelable region, so a
 * `timeout` placed on one of them would have nothing to cancel and would hang along with it. The
 * fibers that never finish are left behind on purpose.
 *
 * The caches are built without a [[cats.effect.Resource]] for the same reason: releasing one runs
 * `clear`, which waits for the entries that are stuck loading, which is precisely what these tests
 * leave behind.
 */
class CacheDefectsSpec extends AsyncFunSuite with Matchers {

  /**
   * After the rewrite: cancelling the load unlinks the key, and the next `getOrUpdate` of it
   * computes a new value.
   */
  test("defect 1: a cancelled load leaves the key unusable") {
    val io = for {
      ref <- Ref[IO].of(EntryRefs.empty[IO, Int, Int])
      cache = LoadingCache(ref)
      started <- Deferred[IO, Unit]
      gate <- Deferred[IO, Unit]
      loader <- cache.getOrUpdate(0) { started.complete(()) *> gate.get.as(1) }.start
      _ <- started.get
      cancelling <- loader.cancel.start
      result <- {
        for {
          cancelled <- cancelling.join.timeout(1.second).attempt
          _ = println(s"defect 1: cancelling the load ended with ${ describe(cancelled) }")
          probe <- cache.getOrUpdate(0)(2.pure[IO]).start
          second <- probe.join.timeout(1.second).attempt
          _ = println(s"defect 1: getOrUpdate of the same key ended with ${ describe(second) }")
          _ = expectedToHang(second)
        } yield ()
      }.guarantee { gate.complete(()) *> cancelling.join.timeout(1.second).attempt.void }
    } yield result
    io.run(timeout = 10.seconds)
  }

  /**
   * After the rewrite: the cleanup evicts the entry once the load runs longer than
   * `Config.loadingTimeout`, and everyone waiting for it fails with `ExpiredError`.
   */
  test("defect 2: an entry stuck loading is never expired") {
    val config = ExpiringCache.Config[IO, Int, Int](expireAfterRead = 100.millis)
    val io = ExpiringCache.of[IO, Int, Int](config).allocated.flatMap { case (cache, _) =>
      for {
        started <- Deferred[IO, Unit]
        gate <- Deferred[IO, Unit]
        loader <- cache.getOrUpdate(0) { started.complete(()) *> gate.get.as(1) }.start
        _ <- started.get
        result <- {
          for {
            _ <- cache.put(1, 1).flatten
            _ <- IO.sleep(500.millis)
            // Control: an ordinary value of the same age is gone, so the cleanup did run.
            control <- cache.contains(1)
            _ = println(s"defect 2: an ordinary entry of the same age is still there: $control")
            _ = control shouldEqual false
            stuck <- cache.contains(0)
            _ = println(s"defect 2: the entry stuck loading is still there: $stuck")
            _ = stuck shouldEqual true
            probe <- cache.getOrUpdate(0)(2.pure[IO]).start
            second <- probe.join.timeout(1.second).attempt
            _ = println(s"defect 2: getOrUpdate of that key ended with ${ describe(second) }")
            _ = expectedToHang(second)
          } yield ()
        }.guarantee { gate.complete(()) *> loader.join.timeout(1.second).attempt.void }
      } yield result
    }
    io.run(timeout = 10.seconds)
  }

  /**
   * After the rewrite: the cancelled load completes its `Deferred` with `CancelledError`, so the
   * waiter fails instead of waiting forever.
   */
  test("defect 3: fibers waiting on a cancelled load are never unblocked") {
    val io = for {
      ref <- Ref[IO].of(EntryRefs.empty[IO, Int, Int])
      cache = LoadingCache(ref)
      started <- Deferred[IO, Unit]
      gate <- Deferred[IO, Unit]
      loader <- cache.getOrUpdate(0) { started.complete(()) *> gate.get.as(1) }.start
      _ <- started.get
      waiter <- cache.getOrUpdate(0)(99.pure[IO]).start
      _ <- IO.sleep(100.millis)
      cancelling <- loader.cancel.start
      result <- {
        for {
          _ <- cancelling.join.timeout(1.second).attempt
          remover <- cache.remove(0).flatten.start
          _ <- remover.join.timeout(1.second).attempt
          outcome <- waiter.join.timeout(1.second).attempt
          _ = println(s"defect 3: the fiber waiting for the value ended with ${ describe(outcome) }")
          _ = expectedToHang(outcome)
        } yield ()
      }.guarantee { gate.complete(()) *> cancelling.join.timeout(1.second).attempt.void }
    } yield result
    io.run(timeout = 10.seconds)
  }

  /**
   * After the rewrite: each key has its own `Ref`, writes of other keys do not invalidate this one,
   * and `getOrUpdate` returns the value.
   */
  test("defect 4: sustained writes of unrelated keys make getOrUpdate fail") {
    val io = for {
      underlying <- Ref[IO].of(EntryRefs.empty[IO, Int, Int])
      counter <- Ref[IO].of(0)
      noise = counter.updateAndGet { _ + 1 }.flatMap { key => insertUnrelated(underlying, key) }
      cache = LoadingCache(intercepted(underlying, noise, none))
      probe <- cache.getOrUpdate(0)(1.pure[IO]).start
      result <- probe.join.timeout(30.seconds).attempt
      _ = println(s"defect 4: getOrUpdate under writes of other keys ended with ${ describe(result) }")
      _ = result match {
        case Right(Outcome.Errored(error)) => error shouldBe an[IllegalStateException]
        case other => fail(s"getOrUpdate survived: ${ describe(other) }")
      }
    } yield ()
    io.run(timeout = 60.seconds)
  }

  /**
   * After the rewrite: one attempt, as the insert of key 1 cannot invalidate the insert of key 0.
   */
  test("defect 4 mechanism: a single insert of an unrelated key forces a retry of getOrUpdate") {
    val io = for {
      underlying <- Ref[IO].of(EntryRefs.empty[IO, Int, Int])
      attempts <- Ref[IO].of(0)
      fired <- Ref[IO].of(false)
      noise = fired.getAndSet(true).flatMap {
        case false => insertUnrelated(underlying, 1)
        case true => IO.unit
      }
      cache = LoadingCache(intercepted(underlying, noise, attempts.some))
      value <- cache.getOrUpdate(0)(1.pure[IO])
      _ = value shouldEqual 1
      attempts <- attempts.get
      _ = println(s"defect 4 mechanism: inserting key 0 took $attempts attempts")
      _ = attempts shouldEqual 2
      keys <- cache.keys
      _ = keys shouldEqual Set(0, 1)
    } yield ()
    io.run()
  }

  /**
   * Asserts that the fiber we joined is still running, i.e. that the timeout hit the join and not
   * the operation.
   */
  private def expectedToHang[A](result: Either[Throwable, Outcome[IO, Throwable, A]]): Any = {
    result
      .swap
      .getOrElse(fail(s"expected to hang, got ${ describe(result) }")) shouldBe a[TimeoutException]
  }

  private def describe[A](result: Either[Throwable, A]): String = {
    result match {
      case Right(a) => s"$a"
      case Left(error) => s"${ error.getClass.getName }: ${ error.getMessage }"
    }
  }

  private def insertUnrelated(underlying: Ref[IO, EntryRefs[IO, Int, Int]], key: Int): IO[Unit] = {
    Ref[IO]
      .of[LoadingCache.EntryState[IO, Int]](
        LoadingCache.EntryState.Value(LoadingCache.Entry(key, none)),
      )
      .flatMap { entryRef => underlying.update { _.updated(key, entryRef) } }
  }

  /**
   * `underlying` with `noise`, a write of some other key, run on every attempt of the cache to
   * modify the map, and the attempts counted in `attempts`.
   *
   * The write lands between the `access` of the map and the `set` that follows it, which is exactly
   * where the single shared `Ref[F, Map[K, EntryRef]]` loses its CAS, so the cache has to start
   * over even though the two operations touch different keys.
   */
  private def intercepted(
    underlying: Ref[IO, EntryRefs[IO, Int, Int]],
    noise: IO[Unit],
    attempts: Option[Ref[IO, Int]],
  ): Ref[IO, EntryRefs[IO, Int, Int]] = {
    type A = EntryRefs[IO, Int, Int]
    val observe = noise *> attempts.foldMapM { _.update { _ + 1 } }
    new Ref[IO, A] {
      def get: IO[A] = underlying.get
      def set(a: A): IO[Unit] = observe *> underlying.set(a)
      def access: IO[(A, A => IO[Boolean])] = {
        underlying.access.map { case (a, set) => (a, (a1: A) => observe *> set(a1)) }
      }
      def tryUpdate(f: A => A): IO[Boolean] = observe *> underlying.tryUpdate(f)
      def tryModify[B](f: A => (A, B)): IO[Option[B]] = observe *> underlying.tryModify(f)
      def update(f: A => A): IO[Unit] = observe *> underlying.update(f)
      def modify[B](f: A => (A, B)): IO[B] = observe *> underlying.modify(f)
      def tryModifyState[B](state: cats.data.State[A, B]): IO[Option[B]] = observe *> underlying.tryModifyState(state)
      def modifyState[B](state: cats.data.State[A, B]): IO[B] = observe *> underlying.modifyState(state)
    }
  }
}
