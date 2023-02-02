package com.evolutiongaming.scache

import cats.{FlatMap, MonadThrow}
import cats.effect.{Concurrent, Ref, Resource}
import cats.kernel.CommutativeMonoid
import cats.syntax.all.*
import com.evolutiongaming.catshelper.CatsHelper.*

import scala.annotation.nowarn

/**
  * Prevents adding new resources to cache after it was released
  */
object CacheFenced {

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
        apply1(cache, ref.get.flatten)
      }
      .fenced
  }

  @deprecated("use `apply1` instead", "4.3.0")
  def apply[F[_] : FlatMap, K, V](cache: Cache[F, K, V], fence: F[Unit]): Cache[F, K, V] = {
    class CacheFenced

    new CacheFenced with Cache[F, K, V] {

      def get(key: K) = cache.get(key)

      def get1(key: K) = cache.get1(key)

      @nowarn("msg=method getOrElse in trait Cache is deprecated")
      def getOrElse(key: K, default: => F[V]) = {
        cache.getOrElse(key, default)
      }

      def getOrUpdate(key: K)(value: => F[V]) = {
        cache.getOrUpdate(key)(value)
      }

      def getOrUpdate1[A](key: K)(value: => F[(A, V, Option[Release])]) = {
        cache.getOrUpdate1(key)(value)
      }

      def getOrUpdateOpt(key: K)(value: => F[Option[V]]) = {
        cache.getOrUpdateOpt(key)(value)
      }

      @nowarn("msg=method getOrUpdateReleasable in trait Cache is deprecated")
      def getOrUpdateReleasable(key: K)(value: => F[Releasable[F, V]]) = {
        cache.getOrUpdateReleasable(key)(fence *> value)
      }

      @nowarn("msg=method getOrUpdateReleasableOpt in trait Cache is deprecated")
      def getOrUpdateReleasableOpt(key: K)(value: => F[Option[Releasable[F, V]]]) = {
        cache.getOrUpdateReleasableOpt(key)(fence *> value)
      }

      def put(key: K, value: V) = cache.put(key, value)

      def put(key: K, value: V, release: Release) = fence.productR { cache.put(key, value, release) }

      def put(key: K, value: V, release: Option[Release]) = cache.put(key, value, release)

      def contains(key: K) = cache.contains(key)

      def size = cache.size

      def keys = cache.keys

      def values = cache.values

      def values1 = cache.values1

      def readyValues = cache.readyValues

      def remove(key: K) = cache.remove(key)

      def clear = cache.clear

      def foldMap[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = cache.foldMap(f)

      def foldMapPar[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = cache.foldMapPar(f)
    }
  }

  def apply1[F[_]: MonadThrow, K, V](cache: Cache[F, K, V], fence: F[Unit]): Cache[F, K, V] = {
    abstract class CacheFenced extends Cache.Abstract1[F, K, V]

    new CacheFenced {

      def get(key: K) = cache.get(key)

      def get1(key: K) = cache.get1(key)

      def getOrUpdate(key: K)(value: => F[V]) = {
        cache.getOrUpdate(key)(value)
      }

      def getOrUpdate1[A](key: K)(value: => F[(A, V, Option[Release])]) = {
        cache.getOrUpdate1(key) {
          value.flatMap {
            case (a, value, Some(release)) => fence.as((a, value, release.some))
            case a                         => a.pure[F]
          }
        }
      }

      def put(key: K, value: V, release: Option[Release]) = {
        release
          .foldMapM { _ => fence }
          .productR { cache.put(key, value, release) }
      }

      def contains(key: K) = cache.contains(key)

      def size = cache.size

      def keys = cache.keys

      def values = cache.values

      def values1 = cache.values1

      def readyValues = cache.readyValues

      def remove(key: K) = cache.remove(key)

      def clear = cache.clear

      def foldMap[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = cache.foldMap(f)

      def foldMapPar[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = cache.foldMapPar(f)
    }
  }

}
