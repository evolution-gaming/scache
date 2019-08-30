package com.evolutiongaming.scache

import cats.Monad
import cats.implicits._

object PartitionedCache {

  def apply[F[_] : Monad, K, V](
    partitions: Partitions[K, Cache[F, K, V]]
  ): Cache[F, K, V] = {

    new Cache[F, K, V] {

      def get(key: K) = {
        val cache = partitions.get(key)
        cache.get(key)
      }

      def getOrUpdate(key: K)(value: => F[V]) = {
        val cache = partitions.get(key)
        cache.getOrUpdate(key)(value)
      }

      def getOrUpdateReleasable(key: K)(value: => F[Releasable[F, V]]) = {
        val cache = partitions.get(key)
        cache.getOrUpdateReleasable(key)(value)
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
        partitions.values.foldMapM(_.clear)
      }
    }
  }
}