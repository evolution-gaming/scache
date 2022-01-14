package com.evolutiongaming.scache

import cats.FlatMap
import cats.effect.{Concurrent, Ref, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.CatsHelper._

/**
  * Prevents adding new resources to cache after it was released
  */
object CacheFenced {

  private sealed abstract class CacheFenced

  def of[F[_] : Concurrent, K, V](cache: Resource[F, Cache[F, K, V]]): Resource[F, Cache[F, K, V]] = {

    val fence = Resource.make {
      Ref[F].of(().pure[F])
    } { fence =>
      fence.set(CacheReleasedError.raiseError[F, Unit])
    }

    val result = for {
      cache <- cache
      fence <- fence
    } yield {
      apply(cache, fence.get.flatten)
    }

    result.fenced
  }

  def apply[F[_] : FlatMap, K, V](cache: Cache[F, K, V], fence: F[Unit]): Cache[F, K, V] = {

    new CacheFenced with Cache[F, K, V] {

      def get(key: K) = cache.get(key)

      def getOrElse(key: K, default: => F[V]) = {
        cache.getOrElse(key, default)
      }

      def getOrUpdate(key: K)(value: => F[V]) = {
        cache.getOrUpdate(key)(value)
      }

      def getOrUpdateOpt(key: K)(value: => F[Option[V]]) = {
        cache.getOrUpdateOpt(key)(value)
      }

      def getOrUpdateReleasable(key: K)(value: => F[Releasable[F, V]]) = {
        cache.getOrUpdateReleasable(key)(fence *> value)
      }

      def getOrUpdateReleasableOpt(key: K)(value: => F[Option[Releasable[F, V]]]) = {
        cache.getOrUpdateReleasableOpt(key)(fence *> value)
      }

      def put(key: K, value: V) = cache.put(key, value)

      def put(key: K, value: V, release: F[Unit]) = fence *> cache.put(key, value, release)

      def contains(key: K) = cache.contains(key)

      def size = cache.size

      def keys = cache.keys

      def values = cache.values

      def remove(key: K) = cache.remove(key)

      def clear = cache.clear
    }
  }
}
