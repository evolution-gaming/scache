package com.evolutiongaming.scache

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, Sync}
import cats.implicits._
import cats.{Applicative, Monad}
import com.evolutiongaming.catshelper.Runtime

trait Cache[F[_], K, V] {

  def get(key: K): F[Option[V]]

  def getOrUpdate(key: K)(value: => F[V]): F[V]

  def put(key: K, value: V): F[Option[F[V]]]

  def remove(key: K): F[Option[F[V]]]

  def clear: F[Unit]

  def values: F[Map[K, F[V]]]
}

object Cache {

  def empty[F[_] : Applicative, K, V]: Cache[F, K, V] = new Cache[F, K, V] {

    def get(key: K) = none[V].pure[F]

    def getOrUpdate(key: K)(value: => F[V]) = value

    def put(key: K, value: V) = none[F[V]].pure[F]

    def remove(key: K) = none[F[V]].pure[F]

    val clear = ().pure[F]

    val values = Map.empty[K, F[V]].pure[F]
  }


  def of[F[_] : Concurrent : Runtime, K, V]: F[Cache[F, K, V]] = {
    for {
      cpus       <- Runtime[F].availableCores
      partitions  = 2 + cpus
      cache      <- of[F, K, V](partitions)
    } yield cache
  }


  def of[F[_] : Concurrent, K, V](nrOfPartitions: Int): F[Cache[F, K, V]] = {
    val cache = of[F, K, V](Map.empty[K, F[V]])
    for {
      partitions <- Partitions.of[F, K, Cache[F, K, V]](nrOfPartitions, _ => cache, _.hashCode())
    } yield {
      apply(partitions)
    }
  }


  def of[F[_] : Concurrent, K, V](map: Map[K, F[V]]): F[Cache[F, K, V]] = {
    for {
      ref <- Ref.of[F, Map[K, F[V]]](map)
    } yield {
      apply(ref)
    }
  }


  private def apply[F[_] : Concurrent, K, V](map: Ref[F, Map[K, F[V]]]): Cache[F, K, V] = {
    new Cache[F, K, V] {

      def get(key: K) = {
        for {
          values <- values
          value  <- values.get(key).fold(none[V].pure[F]) { _.map(_.some) }
        } yield value
      }

      def getOrUpdate(key: K)(value: => F[V]) = {

        def update = {

          def update(deferred: Deferred[F, F[V]]) = {
            Sync[F].uncancelable {
              for {
                value <- value.attempt
                _     <- deferred.complete(value.raiseOrPure[F])
                _     <- value match {
                  case Right(_) => ().pure[F]
                  case Left(_)  => map.update { _ - key }
                }
                value <- value.raiseOrPure[F]
              } yield value
            }
          }

          for {
            deferred <- Deferred[F, F[V]]
            value1   <- map.modify { map =>
              map.get(key).fold {
                val value1 = update(deferred)
                val map1 = map.updated(key, deferred.get.flatten)
                (map1, value1)
              } { value =>
                (map, value)
              }
            }
            value1 <- value1
          } yield value1
        }

        for {
          map   <- map.get
          value <- map.getOrElse(key, update)
        } yield value
      }

      def put(key: K, value: V) = {
        map.modify { map =>
          val value0 = map.get(key)
          val map1 = map.updated(key, value.pure[F])
          (map1, value0)
        }
      }

      def remove(key: K) = {
        map.modify { map =>
          val value = map.get(key)
          val map1 = map - key
          (map1, value)
        }
      }

      val clear = map.set(Map.empty[K, F[V]])

      val values = map.get
    }
  }


  private def apply[F[_] : Monad, K, V](
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

      def put(key: K, value: V) = {
        val cache = partitions.get(key)
        cache.put(key, value)
      }

      def remove(key: K) = {
        val cache = partitions.get(key)
        cache.remove(key)
      }

      val clear = partitions.values.foldMapM(_.clear)

      val values = {
        val zero = Map.empty[K, F[V]]
        partitions.values.foldLeftM(zero) { (a, cache) =>
          for {
            values <- cache.values
          } yield {
            a ++ values
          }
        }
      }
    }
  }
}