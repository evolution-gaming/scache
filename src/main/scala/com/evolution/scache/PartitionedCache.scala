package com.evolution.scache

import cats.syntax.all.*
import cats.{Applicative, Monad, MonadThrow, Parallel}
import com.evolutiongaming.catshelper.ParallelHelper.*
import cats.kernel.{CommutativeMonoid, Monoid}

import scala.annotation.nowarn

object PartitionedCache {

  @deprecated("use `apply1` instead", "4.1.1")
  def apply[F[_]: Monad, K, V](
    partitions: Partitions[K, Cache[F, K, V]]
  ): Cache[F, K, V] = {
    implicit val parallel: Parallel[F] = Parallel.identity
    apply1(partitions)
  }

  @deprecated("use `apply2` instead", "4.3.0")
  def apply1[F[_]: Monad: Parallel, K, V](
    partitions: Partitions[K, Cache[F, K, V]]
  ): Cache[F, K, V] = {

    implicit def monoidUnit: Monoid[F[Unit]] = Applicative.monoid[F, Unit]

    abstract class PartitionedCache extends Cache.Abstract0[F, K, V]

    new PartitionedCache {

      def get(key: K) = {
        partitions
          .get(key)
          .get(key)
      }

      def get1(key: K) = {
        partitions
          .get(key)
          .get1(key)
      }

      def getOrUpdate(key: K)(value: => F[V]) = {
        partitions
          .get(key)
          .getOrUpdate(key)(value)
      }

      def getOrUpdate1[A](key: K)(value: => F[(A, V, Option[Release])]) = {
        partitions
          .get(key)
          .getOrUpdate1(key)(value)
      }

      def getOrUpdateOpt(key: K)(value: => F[Option[V]]) = {
        partitions
          .get(key)
          .getOrUpdateOpt(key)(value)
      }

      @nowarn("msg=deprecated")
      def getOrUpdateReleasableOpt(key: K)(value: => F[Option[Releasable[F, V]]]) = {
        partitions
          .get(key)
          .getOrUpdateReleasableOpt(key)(value)
      }

      def put(key: K, value: V, release: Option[Release]) = {
        partitions
          .get(key)
          .put(key, value, release)
      }

      def contains(key: K) = {
        partitions
          .get(key)
          .contains(key)
      }

      def size = {
        partitions
          .values
          .foldMapM(_.size)
      }

      def keys = {
        partitions
          .values
          .foldLeftM(Set.empty[K]) { (keys, cache) =>
            cache
              .keys
              .map { _ ++ keys }
          }
      }

      def values = {
        partitions
          .values
          .foldLeftM(Map.empty[K, F[V]]) { (values, cache) =>
            cache
              .values
              .map { _ ++ values }
          }
      }

      def values1 = {
        partitions
          .values
          .foldLeftM(Map.empty[K, Either[F[V], V]]) { (values, cache) =>
            cache
              .values1
              .map { _ ++ values }
          }
      }

      def remove(key: K) = {
        partitions
          .get(key)
          .remove(key)
      }

      def clear = {
        partitions
          .values
          .foldMapM { _.clear }
      }

      def foldMap[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = {
        partitions
          .values
          .foldMapM { _.foldMap(f) }
      }

      def foldMapPar[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = {
        partitions
          .values
          .parFoldMap1 { _.foldMap(f) }
      }
    }
  }

  def apply2[F[_]: MonadThrow: Parallel, K, V](
    partitions: Partitions[K, Cache[F, K, V]]
  ): Cache[F, K, V] = {

    implicit def monoidUnit: Monoid[F[Unit]] = Applicative.monoid[F, Unit]

    abstract class PartitionedCache extends Cache.Abstract1[F, K, V]

    new PartitionedCache {

      def get(key: K) = {
        partitions
          .get(key)
          .get(key)
      }

      def get1(key: K) = {
        partitions
          .get(key)
          .get1(key)
      }

      def getOrUpdate(key: K)(value: => F[V]) = {
        partitions
          .get(key)
          .getOrUpdate(key)(value)
      }

      def getOrUpdate1[A](key: K)(value: => F[(A, V, Option[Release])]) = {
        partitions
          .get(key)
          .getOrUpdate1(key)(value)
      }

      def put(key: K, value: V, release: Option[Release]) = {
        partitions
          .get(key)
          .put(key, value, release)
      }

      def contains(key: K) = {
        partitions
          .get(key)
          .contains(key)
      }

      def size = {
        partitions
          .values
          .foldMapM(_.size)
      }

      def keys = {
        partitions
          .values
          .foldLeftM(Set.empty[K]) { (keys, cache) =>
            cache
              .keys
              .map { _ ++ keys }
          }
      }

      def values = {
        partitions
          .values
          .foldLeftM(Map.empty[K, F[V]]) { (values, cache) =>
            cache
              .values
              .map { _ ++ values }
          }
      }

      def values1 = {
        partitions
          .values
          .foldLeftM(Map.empty[K, Either[F[V], V]]) { (values, cache) =>
            cache
              .values1
              .map { _ ++ values }
          }
      }

      def remove(key: K) = {
        partitions
          .get(key)
          .remove(key)
      }

      def clear = {
        partitions
          .values
          .foldMapM { _.clear }
      }

      def foldMap[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = {
        partitions
          .values
          .foldMapM { _.foldMap(f) }
      }

      def foldMapPar[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = {
        partitions
          .values
          .parFoldMap1 { _.foldMap(f) }
      }
    }
  }
}