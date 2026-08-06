package com.evolution.scache.bench

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.evolution.scache.v1.CacheV1
import com.evolution.scache.{Cache, ExpiringCache, LoadingCache, v1}
import org.openjdk.jmh.annotations.*

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

/**
 * Compares the cache before and after it was rebuilt on `MapRef`, measuring both implementations in
 * one run: `impl=v1` is the frozen copy in [[com.evolution.scache.v1]], `impl=v2` is the current
 * one.
 *
 * One invocation is the whole workload, `Fibers` fibers running `OpsPerFiber` cache operations
 * each, so the reported number is cache operations per second, contention included.
 *
 * {{{
 * sbt "benchmark/Jmh/run"
 * sbt "benchmark/Jmh/run -p flavor=partitioned .*getOrUpdateHitRandom.*"
 * }}}
 */
object CacheBenchmark {

  final val Fibers = 8
  final val OpsPerFiber = 20000
  final val Ops = 160000
  final val KeySpace = 10000

  private val fiberIndices = (0 until Fibers).toList
  private val opIndices = (0 until OpsPerFiber).toList

  /**
   * Key of the `n`-th operation of a scenario walking the key space pseudo-randomly.
   *
   * A hash of the operation index rather than a random number, so that every implementation and
   * every iteration sees the very same key sequence, which is what makes the numbers comparable,
   * and so that no shared random generator sits between the fibers and the cache.
   */
  def key(fiber: Int, i: Int): Int = {
    val n = fiber * OpsPerFiber + i
    val h = n * 0x9e3775cd
    ((h ^ (h >>> 16)) & Int.MaxValue) % KeySpace
  }

  def parRun(op: (Int, Int) => IO[Unit]): IO[Unit] = {
    fiberIndices.parTraverse_ { fiber =>
      opIndices.traverse_ { i => op(fiber, i) }
    }
  }
}

/**
 * Cache under benchmark, allocated once per trial.
 *
 * `impl` selects the implementation, `flavor` selects how it is put together: a single unpartitioned
 * [[com.evolution.scache.LoadingCache]], the partitioned `Cache.loading`, or the partitioned
 * `Cache.expiring` with expiration far enough away not to interfere.
 */
@State(Scope.Benchmark)
abstract class CacheState {

  @Param(Array("v1", "v2"))
  var impl: String = "v2"

  @Param(Array("single", "partitioned", "expiring"))
  var flavor: String = "partitioned"

  var cache: Cache[IO, Int, Int] = null

  private var release: IO[Unit] = IO.unit

  private def resource = {
    val expireAfterRead = 1.hour
    (impl, flavor) match {
      case ("v1", "single") => v1.LoadingCache.of(v1.LoadingCache.EntryRefs.empty[IO, Int, Int])
      case ("v1", "partitioned") => CacheV1.loading[IO, Int, Int]()
      case ("v1", "expiring") => CacheV1.expiring[IO, Int, Int](v1.ExpiringCache.Config[IO, Int, Int](expireAfterRead))
      case ("v2", "single") => LoadingCache.of[IO, Int, Int]
      case ("v2", "partitioned") => Cache.loading[IO, Int, Int]
      case ("v2", "expiring") => Cache.expiring[IO, Int, Int](ExpiringCache.Config[IO, Int, Int](expireAfterRead))
      case (impl, flavor) => sys.error(s"unknown impl=$impl flavor=$flavor")
    }
  }

  @Setup(Level.Trial)
  def allocate(): Unit = {
    val (cache, release) = resource.allocated.unsafeRunSync()
    this.cache = cache
    this.release = release
  }

  @TearDown(Level.Trial)
  def free(): Unit = release.unsafeRunSync()
}

/**
 * Cache emptied before every invocation, so that the scenarios adding keys always take the path of
 * a missing key.
 */
@State(Scope.Benchmark)
class EmptyCacheState extends CacheState {

  @Setup(Level.Invocation)
  def empty(): Unit = cache.clear.flatten.unsafeRunSync()
}

/**
 * Cache holding the whole key space, refilled between the iterations, so that the scenarios reading
 * or replacing keys always take the path of a present key.
 */
@State(Scope.Benchmark)
class PopulatedCacheState extends CacheState {

  @Setup(Level.Iteration)
  def populate(): Unit = {
    (0 until CacheBenchmark.KeySpace)
      .toList
      .traverse_ { key => cache.put(key, key).flatten }
      .unsafeRunSync()
  }
}

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@OperationsPerInvocation(160000)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
class CacheBenchmark {

  import CacheBenchmark.*

  @Benchmark
  def getOrUpdateInsertDistinctKeys(state: EmptyCacheState): Unit = {
    parRun { (fiber, i) =>
      val key = fiber * OpsPerFiber + i
      state.cache.getOrUpdate(key)(key.pure[IO]).void
    }.unsafeRunSync()
  }

  @Benchmark
  def putInsertDistinctKeys(state: EmptyCacheState): Unit = {
    parRun { (fiber, i) =>
      val key = fiber * OpsPerFiber + i
      state.cache.put(key, i).flatten.void
    }.unsafeRunSync()
  }

  @Benchmark
  def modifyInsertDistinctKeys(state: EmptyCacheState): Unit = {
    parRun { (fiber, i) =>
      val key = fiber * OpsPerFiber + i
      state.cache.modify(key) { _ => ((), Cache.Directive.Put(i, none)) }.void
    }.unsafeRunSync()
  }

  /**
   * Not the same as [[getHitRandomKeys]]: `getOrUpdate` of a key that is already there still has to
   * decide between a hit and a miss, which is where the old implementation touched the shared `Ref`
   * even though it ended up returning a cached value.
   */
  @Benchmark
  def getOrUpdateHitRandomKeys(state: PopulatedCacheState): Unit = {
    parRun { (fiber, i) =>
      val k = key(fiber, i)
      state.cache.getOrUpdate(k)(k.pure[IO]).void
    }.unsafeRunSync()
  }

  @Benchmark
  def getOrUpdateHitSingleHotKey(state: PopulatedCacheState): Unit = {
    parRun { (_, _) => state.cache.getOrUpdate(0)(0.pure[IO]).void }.unsafeRunSync()
  }

  @Benchmark
  def getHitRandomKeys(state: PopulatedCacheState): Unit = {
    parRun { (fiber, i) => state.cache.get(key(fiber, i)).void }.unsafeRunSync()
  }

  @Benchmark
  def get1HitRandomKeys(state: PopulatedCacheState): Unit = {
    parRun { (fiber, i) => state.cache.get1(key(fiber, i)).void }.unsafeRunSync()
  }

  @Benchmark
  def containsRandomKeys(state: PopulatedCacheState): Unit = {
    parRun { (fiber, i) => state.cache.contains(key(fiber, i)).void }.unsafeRunSync()
  }

  @Benchmark
  def putReplaceRandomKeys(state: PopulatedCacheState): Unit = {
    parRun { (fiber, i) => state.cache.put(key(fiber, i), i).flatten.void }.unsafeRunSync()
  }

  @Benchmark
  def modifyUpdateRandomKeys(state: PopulatedCacheState): Unit = {
    parRun { (fiber, i) =>
      state
        .cache
        .modify(key(fiber, i)) {
          case Some(value) => ((), Cache.Directive.Put(value + 1, none))
          case None => ((), Cache.Directive.Ignore)
        }
        .void
    }.unsafeRunSync()
  }

  @Benchmark
  def removeAndPutRandomKeys(state: PopulatedCacheState): Unit = {
    parRun { (fiber, i) =>
      val k = key(fiber, i)
      state.cache.remove(k).flatten *> state.cache.put(k, i).flatten.void
    }.unsafeRunSync()
  }

  @Benchmark
  def mixedRandomKeys(state: PopulatedCacheState): Unit = {
    parRun { (fiber, i) =>
      val k = key(fiber, i)
      (i % 10) match {
        case 0 => state.cache.put(k, i).flatten.void
        case 1 => state.cache.remove(k).flatten.void
        case 2 => state.cache.modify(k) { _ => ((), Cache.Directive.Put(i, none)) }.void
        case 3 | 4 => state.cache.get(k).void
        case _ => state.cache.getOrUpdate(k)(i.pure[IO]).void
      }
    }.unsafeRunSync()
  }

  /**
   * Enumeration of the whole cache, one full traversal of `KeySpace` entries per operation, hence
   * measured per traversal rather than per key.
   */
  @Benchmark
  @OperationsPerInvocation(1)
  def foldMapWholeCache(state: PopulatedCacheState): Unit = {
    state
      .cache
      .foldMap { case (_, value) => value.fold(identity, _.pure[IO]) }
      .void
      .unsafeRunSync()
  }
}
