package com.evolutiongaming.scache

import cats.{Applicative, Monad}
import cats.syntax.all._

object PartitionedCache {

  def apply[F[_] : Monad, K, V](
    partitions: Partitions[K, Cache[F, K, V]]
  ): Cache[F, K, V] = {

    implicit val monoidUnit = Applicative.monoid[F, Unit]

    new Cache[F, K, V] {

      def get(key: K) = {
        val cache = partitions.get(key)
        cache.get(key)
      }

      def getOrElse(key: K, default: => F[V]) = {
        val cache = partitions.get(key)
        cache.getOrElse(key, default)
      }

      def getOrUpdate(key: K)(value: => F[V]) = {
        val cache = partitions.get(key)
        cache.getOrUpdate(key)(value)
      }

      def getOrUpdateOpt(key: K)(value: => F[Option[V]]) = {
        val cache = partitions.get(key)
        cache.getOrUpdateOpt(key)(value)
      }

      def getOrUpdateReleasable(key: K)(value: => F[Releasable[F, V]]) = {
        val cache = partitions.get(key)
        cache.getOrUpdateReleasable(key)(value)
      }

      def getOrUpdateReleasableOpt(key: K)(value: => F[Option[Releasable[F, V]]]) = {
        val cache = partitions.get(key)
        cache.getOrUpdateReleasableOpt(key)(value)
      }

      def put(key: K, value: V) = {
        val cache = partitions.get(key)
        cache.put(key, value)
      }

      def put(key: K, value: V, release: F[Unit]) = {
        val cache = partitions.get(key)
        cache.put(key, value, release)
      }

      val size = {
        partitions.values.foldMapM(_.size)
      }

      val keys = {
        val zero = Set.empty[K]
        partitions.values.foldLeftM(zero) { (result, cache) =>
          for {
            keys <- cache.keys
          } yield {
            result ++ keys
          }
        }
      }

      def contains(key: K) = {
        this.keys.map(_.contains(key))
      }

      val values = {
        val zero = Map.empty[K, F[V]]
        partitions.values.foldLeftM(zero) { (result, cache) =>
          for {
            values <- cache.values
          } yield {
            result ++ values
          }
        }
      }

      def remove(key: K) = {
        val cache = partitions.get(key)
        cache.remove(key)
      }

      val clear = {
        for {
          clear <- partitions.values.traverse(_.clear)
        } yield {
          clear.combineAll
        }
      }
    }
  }
}