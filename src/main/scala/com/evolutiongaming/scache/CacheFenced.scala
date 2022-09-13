package com.evolutiongaming.scache

import cats.FlatMap
import cats.effect.{Concurrent, Ref, Resource}
import cats.kernel.CommutativeMonoid
import cats.syntax.all.*
import com.evolutiongaming.catshelper.CatsHelper.*

/**
  * Prevents adding new resources to cache after it was released
  */
object CacheFenced {

  private sealed abstract class CacheFenced

  @deprecated("use `Cache[F, K, V].withFence`", "4.1.1")
  def of[F[_] : Concurrent, K, V](cache: Resource[F, Cache[F, K, V]]): Resource[F, Cache[F, K, V]] = {
    cache.flatMap { cache => of1(cache)}
  }

  def of1[F[_]: Concurrent, K, V](cache: Cache[F, K, V]): Resource[F, Cache[F, K, V]] = {
    Resource
      .make {
        Ref[F].of(().pure[F])
      } { fence =>
        fence.set(CacheReleasedError.raiseError[F, Unit])
      }
      .map { ref =>
        apply(cache, ref.get.flatten)
      }
      .fenced
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

      def values1 = cache.values1

      def remove(key: K) = cache.remove(key)

      def clear = cache.clear

      def foldMap[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = cache.foldMap(f)

      def foldMapPar[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = cache.foldMapPar(f)
    }
  }
}
