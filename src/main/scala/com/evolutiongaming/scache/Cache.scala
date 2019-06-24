package com.evolutiongaming.scache

import cats.Applicative
import cats.effect.{Concurrent, Resource, Timer}
import cats.implicits._
import cats.temp.par.Par
import com.evolutiongaming.catshelper.Runtime

import scala.concurrent.duration._

trait Cache[F[_], K, V] {

  def get(key: K): F[Option[V]]

  /**
    * Does not run `value` concurrently for the same key
    */
  def getOrUpdate(key: K)(value: => F[V]): F[V]

  /**
    * @return previous value if any, possibly not yet loaded
    */
  def put(key: K, value: V): F[Option[F[V]]]


  def size: F[Int]


  def keys: F[Set[K]]

  /**
    * Might be an expensive call
    */
  def values: F[Map[K, F[V]]]

  /**
    * @return previous value if any, possibly not yet loaded
    */
  def remove(key: K): F[Option[F[V]]]


  /**
    * Removes loading values from the cache, however does not cancel them
    */
  def clear: F[Unit]
}

object Cache {

  def empty[F[_] : Applicative, K, V]: Cache[F, K, V] = new Cache[F, K, V] {

    def get(key: K) = none[V].pure[F]

    def getOrUpdate(key: K)(value: => F[V]) = value

    def put(key: K, value: V) = none[F[V]].pure[F]

    val size = 0.pure[F]

    val keys = Set.empty[K].pure[F]

    val values = Map.empty[K, F[V]].pure[F]

    def remove(key: K) = none[F[V]].pure[F]

    val clear = ().pure[F]
  }


  def loading[F[_] : Concurrent : Runtime : Timer, K, V](): F[Cache[F, K, V]] = {
    for {
      nrOfPartitions <- NrOfPartitions[F]()
      cache           = LoadingCache.of(LoadingCache.EntryRefs.empty[F, K, V])
      partitions     <- Partitions.of[F, K, Cache[F, K, V]](nrOfPartitions, _ => cache, _.hashCode())
    } yield {
      PartitionedCache(partitions)
    }
  }


  def expiring[F[_] : Concurrent : Timer : Runtime : Par, K, V](
    expireAfter: FiniteDuration,
    maxSize: Option[Int] = None,
    refresh: Option[ExpiringCache.Refresh[K, F[V]]] = None
  ): Resource[F, Cache[F, K, V]] = {

    type G[A] = Resource[F, A]

    for {
      nrOfPartitions <- Resource.liftF(NrOfPartitions[F]())
      maxSize1        = maxSize.map { maxSize => (maxSize * 1.1 / nrOfPartitions).toInt }
      cache           = ExpiringCache.of[F, K, V](expireAfter, maxSize1, refresh)
      partitions     <- Partitions.of[G, K, Cache[F, K, V]](nrOfPartitions, _ => cache, _.hashCode())
    } yield {
      PartitionedCache(partitions)
    }
  }
}