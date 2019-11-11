package com.evolutiongaming.scache

import cats.effect.{Concurrent, Resource, Timer}
import cats.implicits._
import cats.kernel.Hash
import cats.{Functor, Monad, Parallel, ~>}
import com.evolutiongaming.catshelper.Runtime
import com.evolutiongaming.smetrics.MeasureDuration

import scala.concurrent.duration._

trait Cache[F[_], K, V] {

  def get(key: K): F[Option[V]]

  /**
    * Does not run `value` concurrently for the same key
    */
  def getOrUpdate(key: K)(value: => F[V]): F[V]

  /**
    * Does not run `value` concurrently for the same key
    * Releasable.release will be called upon key removal from the cache
    */
  def getOrUpdateReleasable(key: K)(value: => F[Releasable[F, V]]): F[V]

  /**
    * @return previous value if any, possibly not yet loaded
    */
  def put(key: K, value: V): F[F[Option[V]]]


  def put(key: K, value: V, release: F[Unit]): F[F[Option[V]]]


  def size: F[Int]


  def keys: F[Set[K]]

  /**
    * Might be an expensive call
    */
  def values: F[Map[K, F[V]]]

  /**
    * @return previous value if any, possibly not yet loaded
    */
  def remove(key: K): F[F[Option[V]]]


  /**
    * Removes loading values from the cache, however does not cancel them
    */
  def clear: F[F[Unit]]
}

object Cache {

  def empty[F[_] : Monad, K, V]: Cache[F, K, V] = new Cache[F, K, V] {

    def get(key: K) = none[V].pure[F]

    def getOrUpdate(key: K)(value: => F[V]) = value

    def getOrUpdateReleasable(key: K)(value: => F[Releasable[F, V]]) = value.map(_.value)

    def put(key: K, value: V) = none[V].pure[F].pure[F]

    def put(key: K, value: V, release: F[Unit]) = none[V].pure[F].pure[F]

    val size = 0.pure[F]

    val keys = Set.empty[K].pure[F]

    val values = Map.empty[K, F[V]].pure[F]

    def remove(key: K) = none[V].pure[F].pure[F]

    val clear = ().pure[F].pure[F]
  }


  def loading[F[_] : Concurrent : Runtime, K, V]: Resource[F, Cache[F, K, V]] = {

    type G[A] = Resource[F, A]

    implicit val hash = Hash.fromUniversalHashCode[K]

    for {
      nrOfPartitions <- Resource.liftF(NrOfPartitions[F]())
      cache           = LoadingCache.of(LoadingCache.EntryRefs.empty[F, K, V])
      partitions     <- Partitions.of[G, K, Cache[F, K, V]](nrOfPartitions, _ => cache)
    } yield {
      PartitionedCache(partitions)
    }
  }


  def expiring[F[_] : Concurrent : Timer : Runtime : Parallel, K, V](
    expireAfter: FiniteDuration,
    maxSize: Option[Int] = None,
    refresh: Option[ExpiringCache.Refresh[K, F[V]]] = None
  ): Resource[F, Cache[F, K, V]] = {

    type G[A] = Resource[F, A]

    implicit val hash = Hash.fromUniversalHashCode[K]

    for {
      nrOfPartitions <- Resource.liftF(NrOfPartitions[F]())
      maxSize1        = maxSize.map { maxSize => (maxSize * 1.1 / nrOfPartitions).toInt }
      cache           = ExpiringCache.of[F, K, V](expireAfter, maxSize1, refresh)
      partitions     <- Partitions.of[G, K, Cache[F, K, V]](nrOfPartitions, _ => cache)
    } yield {
      PartitionedCache(partitions)
    }
  }


  implicit class CacheOps[F[_], K, V](val self: Cache[F, K, V]) extends AnyVal {

    def withMetrics(
      metrics: CacheMetrics[F])(implicit
      F: Concurrent[F],
      timer: Timer[F],
      measureDuration: MeasureDuration[F]
    ): Resource[F, Cache[F, K, V]] = {
      CacheMetered(self, metrics)
    }

    def mapK[G[_]](fg: F ~> G, gf: G ~> F)(implicit F: Functor[F]): Cache[G, K, V] = new Cache[G, K, V] {

      def get(key: K) = fg(self.get(key))

      def getOrUpdate(key: K)(value: => G[V]) = fg(self.getOrUpdate(key)(gf(value)))

      def getOrUpdateReleasable(key: K)(value: => G[Releasable[G, V]]) = {
        fg(self.getOrUpdateReleasable(key)(gf(value).map(_.mapK(gf))))
      }

      def put(key: K, value: V) = fg(self.put(key, value).map(fg.apply))

      def put(key: K, value: V, release: G[Unit]) = fg(self.put(key, value, gf(release)).map(fg.apply))

      def size = fg(self.size)

      def keys = fg(self.keys)

      def values = fg(self.values.map(_.map { case (k, v) => (k, fg(v))} ) )

      def remove(key: K) = fg(self.remove(key).map(fg.apply))

      def clear = fg(self.clear.map(fg.apply))
    }
  }


  implicit class CacheResourceOps[F[_], K, V](val self: Resource[F, Cache[F, K, V]]) extends AnyVal {

    def withFence(implicit F: Concurrent[F]): Resource[F, Cache[F, K, V]] = CacheFenced.of(self)
  }
}