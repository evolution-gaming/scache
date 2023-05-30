package com.evolution.scache

import cats.MonadThrow
import cats.effect.{Concurrent, Ref, Resource}
import cats.kernel.CommutativeMonoid
import cats.syntax.all.*
import com.evolutiongaming.catshelper.CatsHelper.*

/**
  * Prevents adding new resources to cache after it was released
  */
object CacheFenced {

  def of[F[_]: Concurrent, K, V](cache: Cache[F, K, V]): Resource[F, Cache[F, K, V]] = {
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

  def apply[F[_]: MonadThrow, K, V](cache: Cache[F, K, V], fence: F[Unit]): Cache[F, K, V] = {
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

      def modify[A](key: K, f: Option[V] => (A, Cache.Directive[F, V])): F[(A, Option[F[Unit]])] =
        fence.flatMap(_ => cache.modify(key, f)) // TODO: this doesn't guarantee a race, though

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
