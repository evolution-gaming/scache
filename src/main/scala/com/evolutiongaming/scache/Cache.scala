package com.evolutiongaming.scache

import cats.effect.{Concurrent, Resource, Timer}
import cats.syntax.all._
import cats.kernel.{CommutativeMonoid, Hash, Monoid}
import cats.{Functor, Monad, Parallel, ~>}
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.Runtime
import com.evolutiongaming.smetrics.MeasureDuration

import scala.concurrent.duration._

trait Cache[F[_], K, V] {

  def get(key: K): F[Option[V]]

  def get1(key: K): F[Option[Either[F[V], V]]]

  def getOrElse(key: K, default: => F[V]): F[V]

  /**
    * Does not run `value` concurrently for the same key
    */
  def getOrUpdate(key: K)(value: => F[V]): F[V]

  /**
    * Does not run `value` concurrently for the same key
    * In case of none returned, value will be ignored by cache
    */
  def getOrUpdateOpt(key: K)(value: => F[Option[V]]): F[Option[V]]

  /**
    * Does not run `value` concurrently for the same key
    * Releasable.release will be called upon key removal from the cache
    */
  def getOrUpdateReleasable(key: K)(value: => F[Releasable[F, V]]): F[V]

  /**
    * Does not run `value` concurrently for the same key
    * Releasable.release will be called upon key removal from the cache
    * In case of none returned, value will be ignored by cache
    */
  def getOrUpdateReleasableOpt(key: K)(value: => F[Option[Releasable[F, V]]]): F[Option[V]]

  /**
    * @return previous value if any, possibly not yet loaded
    */
  def put(key: K, value: V): F[F[Option[V]]]


  def put(key: K, value: V, release: F[Unit]): F[F[Option[V]]]


  def contains(key: K): F[Boolean]
  

  def size: F[Int]


  def keys: F[Set[K]]

  /**
    * Might be an expensive call
    */
  def values: F[Map[K, F[V]]]

  /**
    * @return either map of either loading pr loaded value
    * Might be an expensive call
    */
  def values1: F[Map[K, Either[F[V], V]]]

  /**
    * @return previous value if any, possibly not yet loaded
    */
  def remove(key: K): F[F[Option[V]]]


  /**
    * Removes loading values from the cache, however does not cancel them
    */
  def clear: F[F[Unit]]

  def foldMap[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]): F[A]

  def foldMapPar[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]): F[A]
}

object Cache {

  def empty[F[_]: Monad, K, V]: Cache[F, K, V] = {
    class Empty
    new Empty with Cache[F, K, V] {

      def get(key: K) = none[V].pure[F]

      def get1(key: K) = none[Either[F[V], V]].pure[F]

      def getOrElse(key: K, default: => F[V]): F[V] = default

      def getOrUpdate(key: K)(value: => F[V]) = value

      def getOrUpdateOpt(key: K)(value: => F[Option[V]]) = value

      def getOrUpdateReleasable(key: K)(value: => F[Releasable[F, V]]) = value.map(_.value)

      def getOrUpdateReleasableOpt(key: K)(value: => F[Option[Releasable[F, V]]]) = {
        value.map { _.map(_.value) }
      }

      def put(key: K, value: V) = none[V].pure[F].pure[F]

      def put(key: K, value: V, release: F[Unit]) = none[V].pure[F].pure[F]

      def contains(key: K) = false.pure[F]

      def size = 0.pure[F]

      def keys = Set.empty[K].pure[F]

      def values = Map.empty[K, F[V]].pure[F]

      def values1 = Map.empty[K, Either[F[V], V]].pure[F]

      def remove(key: K) = none[V].pure[F].pure[F]

      def clear = ().pure[F].pure[F]

      def foldMap[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = Monoid[A].empty.pure[F]

      def foldMapPar[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = Monoid[A].empty.pure[F]
    }
  }

  @deprecated("use `loading1` instead", "4.1.1")
  def loading[F[_]: Concurrent: Runtime, K, V]: Resource[F, Cache[F, K, V]] = {
    implicit val parallel: Parallel[F] = Parallel.identity
    loading1(none)
  }

  @deprecated("use `loading1` instead", "4.1.1")
  def loading[F[_]: Concurrent: Runtime, K, V](partitions: Int): Resource[F, Cache[F, K, V]] = {
    implicit val parallel: Parallel[F] = Parallel.identity
    loading1(partitions.some)
  }

  @deprecated("use `loading1` instead", "4.1.1")
  def loading[F[_]: Concurrent: Runtime, K, V](partitions: Option[Int] = None): Resource[F, Cache[F, K, V]] = {
    implicit val parallel: Parallel[F] = Parallel.identity
    loading1[F, K, V](partitions)
  }

  def loading1[F[_]: Concurrent: Parallel: Runtime, K, V]: Resource[F, Cache[F, K, V]] = {
    loading1(none)
  }

  def loading1[F[_]: Concurrent: Parallel: Runtime, K, V](partitions: Int): Resource[F, Cache[F, K, V]] = {
    loading1(partitions.some)
  }

  def loading1[F[_]: Concurrent: Parallel: Runtime, K, V](partitions: Option[Int] = None): Resource[F, Cache[F, K, V]] = {

    implicit val hash = Hash.fromUniversalHashCode[K]

    val result = for {
      nrOfPartitions <- partitions
        .map { _.pure[F] }
        .getOrElse { NrOfPartitions[F]() }
        .toResource
      cache           = LoadingCache.of(LoadingCache.EntryRefs.empty[F, K, V])
      partitions     <- Partitions.of[Resource[F, *], K, Cache[F, K, V]](nrOfPartitions, _ => cache)
    } yield {
      fromPartitions(partitions)
    }
    result.breakFlatMapChain
  }


  @deprecated("use `expiring` with `config` argument", "2.3.0")
  def expiring[F[_]: Concurrent: Timer: Runtime: Parallel, K, V](
    expireAfter: FiniteDuration,
  ): Resource[F, Cache[F, K, V]] = {
    expiring(
      ExpiringCache.Config(expireAfterRead = expireAfter),
      none)
  }

  @deprecated("use `expiring` with `config` argument", "2.3.0")
  def expiring[F[_]: Concurrent: Timer: Runtime: Parallel, K, V](
    expireAfter: FiniteDuration,
    maxSize: Option[Int],
    refresh: Option[ExpiringCache.Refresh[K, F[V]]]
  ): Resource[F, Cache[F, K, V]] = {
    expiring(
      ExpiringCache.Config(
        expireAfterRead = expireAfter,
        maxSize = maxSize,
        refresh = refresh.map { refresh => refresh.copy(value = (k: K) => refresh.value(k).map { _.some }) }),
      none)
  }


  @deprecated("use `expiring` with `config` argument", "2.3.0")
  def expiring[F[_]: Concurrent: Timer: Runtime: Parallel, K, V](
    expireAfter: FiniteDuration,
    maxSize: Option[Int],
    refresh: Option[ExpiringCache.Refresh[K, F[V]]],
    partitions: Option[Int]
  ): Resource[F, Cache[F, K, V]] = {
    expiring(
      ExpiringCache.Config(
        expireAfterRead = expireAfter,
        maxSize = maxSize,
        refresh = refresh.map { refresh => refresh.copy(value = (k: K) => refresh.value(k).map { _.some }) }),
      partitions)
  }


  def expiring[F[_]: Concurrent: Timer: Runtime: Parallel, K, V](
    config: ExpiringCache.Config[F, K, V],
    partitions: Option[Int] = None
  ): Resource[F, Cache[F, K, V]] = {

    implicit val hash = Hash.fromUniversalHashCode[K]

    val result = for {
      nrOfPartitions <- partitions
        .map { _.pure[F] }
        .getOrElse { NrOfPartitions[F]() }
        .toResource
      config1         = config
        .maxSize
        .fold {
          config
        } {
          maxSize => config.copy(maxSize = (maxSize * 1.1 / nrOfPartitions).toInt.some)
        }
      cache           = ExpiringCache.of[F, K, V](config1)
      partitions     <- Partitions.of[Resource[F, *], K, Cache[F, K, V]](nrOfPartitions, _ => cache)
    } yield {
      fromPartitions(partitions)
    }

    result.breakFlatMapChain
  }

  def fromPartitions[F[_]: Monad: Parallel, K, V](partitions: Partitions[K, Cache[F, K, V]]): Cache[F, K, V] = {
    PartitionedCache.apply1(partitions)
  }

  private sealed abstract class MapK

  implicit class CacheOps[F[_], K, V](val self: Cache[F, K, V]) extends AnyVal {

    def withMetrics(
      metrics: CacheMetrics[F])(implicit
      F: Concurrent[F],
      timer: Timer[F],
      measureDuration: MeasureDuration[F]
    ): Resource[F, Cache[F, K, V]] = {
      CacheMetered(self, metrics)
    }

    def mapK[G[_]](fg: F ~> G, gf: G ~> F)(implicit F: Functor[F]): Cache[G, K, V] = {
      new MapK with Cache[G, K, V] {

        def get(key: K) = fg(self.get(key))

        def get1(key: K) = {
          fg {
            self
              .get1(key)
              .map { _.map { _.leftMap { a => fg(a) } } }
          }
        }

        def getOrElse(key: K, default: => G[V]) = fg(self.getOrElse(key, gf(default)))

        def getOrUpdate(key: K)(value: => G[V]) = fg(self.getOrUpdate(key)(gf(value)))

        def getOrUpdateOpt(key: K)(value: => G[Option[V]]) = {
          fg(self.getOrUpdateOpt(key)(gf(value)))
        }

        def getOrUpdateReleasable(key: K)(value: => G[Releasable[G, V]]) = {
          fg(self.getOrUpdateReleasable(key)(gf(value).map(_.mapK(gf))))
        }

        def getOrUpdateReleasableOpt(key: K)(value: => G[Option[Releasable[G, V]]]) = {
          fg(self.getOrUpdateReleasableOpt(key)(gf(value).map(_.map(_.mapK(gf)))))
        }

        def put(key: K, value: V) = fg(self.put(key, value).map(fg.apply))

        def put(key: K, value: V, release: G[Unit]) = fg(self.put(key, value, gf(release)).map(fg.apply))

        def contains(key: K) = fg(self.contains(key))

        def size = fg(self.size)

        def keys = fg(self.keys)

        def values = fg(self.values.map(_.map { case (k, v) => (k, fg(v)) }))

        def values1 = fg {
          self
            .values1
            .map { _.map { case (k, v) => (k, v.leftMap { v => fg(v) }) } }
        }

        def remove(key: K) = fg(self.remove(key).map(fg.apply))

        def clear = fg(self.clear.map(fg.apply))

        def foldMap[A: CommutativeMonoid](f: (K, Either[G[V], V]) => G[A]) = {
          fg(self.foldMap { case (k, v) => gf(f(k, v.leftMap { v => fg(v) })) })
        }

        def foldMapPar[A: CommutativeMonoid](f: (K, Either[G[V], V]) => G[A]) = {
          fg(self.foldMap { case (k, v) => gf(f(k, v.leftMap { v => fg(v) })) })
        }
      }
    }

    def withFence(implicit F: Concurrent[F]): Resource[F, Cache[F, K, V]] = CacheFenced.of1(self)
  }


  implicit class CacheResourceOps[F[_], K, V](val self: Resource[F, Cache[F, K, V]]) extends AnyVal {

    @deprecated("use `Cache[F, K, V].withFence`", "4.1.1")
    def withFence(implicit F: Concurrent[F]): Resource[F, Cache[F, K, V]] = CacheFenced.of(self)
  }
}