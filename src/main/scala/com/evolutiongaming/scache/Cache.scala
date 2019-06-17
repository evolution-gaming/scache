package com.evolutiongaming.scache

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, Sync}
import cats.implicits._
import cats.{Applicative, Monad}
import com.evolutiongaming.catshelper.Runtime

trait Cache[F[_], K, V] {

  def get(key: K): F[Option[V]]

  def getOrUpdate(key: K)(value: => F[V]): F[V]

  /**
    * @return previous value if any, possibly not yet loaded
    */
  def put(key: K, value: V): F[Option[F[V]]]

  def keys: F[Set[K]]
  
  /**
    * Might be an expensive call
    */
  def values: F[Map[K, F[V]]]

  /**
    * @return previous value if any, possibly not yet loaded
    */
  def remove(key: K): F[Option[F[V]]]

  def clear: F[Unit]
}

object Cache {

  def empty[F[_] : Applicative, K, V]: Cache[F, K, V] = new Cache[F, K, V] {

    def get(key: K) = none[V].pure[F]

    def getOrUpdate(key: K)(value: => F[V]) = value

    def put(key: K, value: V) = none[F[V]].pure[F]

    val keys = Set.empty[K].pure[F]

    val values = Map.empty[K, F[V]].pure[F]

    def remove(key: K) = none[F[V]].pure[F]

    val clear = ().pure[F]
  }


  def of[F[_] : Concurrent : Runtime, K, V]: F[Cache[F, K, V]] = {
    for {
      cpus       <- Runtime[F].availableCores
      partitions  = 2 + cpus
      cache      <- of[F, K, V](partitions)
    } yield cache
  }


  def of[F[_] : Concurrent, K, V](nrOfPartitions: Int): F[Cache[F, K, V]] = {
    val cache = of(EntryRefs.empty[F, K, V])
    for {
      partitions <- Partitions.of[F, K, Cache[F, K, V]](nrOfPartitions, _ => cache, _.hashCode())
    } yield {
      apply(partitions)
    }
  }


  def of[F[_] : Concurrent, K, V](map: EntryRefs[F, K, V]): F[Cache[F, K, V]] = {
    for {
      ref <- Ref[F].of(map)
    } yield {
      apply(ref)
    }
  }


  private[scache] def apply[F[_] : Concurrent, K, V](ref: Ref[F, EntryRefs[F, K, V]]): Cache[F, K, V] = {
    new Cache[F, K, V] {

      def get(key: K) = {
        for {
          map   <- ref.get
          value <- map.get(key).value
        } yield value
      }

      def getOrUpdate(key: K)(value: => F[V]) = {

        def update = {

          def completeOf(entryRef: EntryRef[F, V], deferred: Deferred[F, F[V]]) = {
            for {
              value <- value.attempt
              _     <- value match {
                case Right(value) => entryRef.update {
                  case a: Entry.Loaded[F, V]  => a
                  case _: Entry.Loading[F, V] => Entry.loaded(value)
                }
                case Left(_)  => ref.update { _ - key }
              }
              _     <- deferred.complete(value.raiseOrPure[F])
              value <- value.raiseOrPure[F]
            } yield value
          }

          def update(entryRef: EntryRef[F, V], complete: F[V]): F[V] = {
            Sync[F].uncancelable {
              for {
                value <- ref.modify { map =>
                  map.get(key).fold {
                    val map1 = map.updated(key, entryRef)
                    (map1, complete)
                  } { entryRef =>
                    (map, entryRef.value)
                  }
                }
                value <- value
              } yield value
            }
          }

          for {
            deferred  <- Deferred[F, F[V]]
            entryRef  <- Ref[F].of(Entry.loading(deferred))
            complete   = completeOf(entryRef, deferred)
            value     <- update(entryRef, complete)
          } yield value
        }

        for {
          map   <- ref.get
          value <- map.get(key).fold { update } { _.value }
        } yield value
      }


      def put(key: K, value: V) = {

        val entry = Entry.loaded[F, V](value)

        def add = {
          for {
            entryRef  <- Ref[F].of(entry)
            entryRef0 <- ref.modify { map =>
              val entryRef0 = map.get(key)
              val map1 = map.updated(key, entryRef)
              (map1, entryRef0)
            }
          } yield for {
            entryRef0 <- entryRef0
          } yield {
            entryRef0.value
          }
        }

        def update(entryRef: EntryRef[F, V]) = {
          for {
            entry0 <- entryRef.getAndSet(entry)
          } yield {
            entry0.value.some
          }
        }

        for {
          map    <- ref.get
          value0 <- map.get(key).fold(add)(update)
        } yield value0
      }

      val keys = {
        for {
          map <- ref.get
        } yield {
          map.keySet
        }
      }

      val values = {
        for {
          map <- ref.get
        } yield {
          map.mapValues(_.value)
        }
      }

      def remove(key: K) = {
        for {
          entryRef <- ref.modify { map =>
            val entryRef = map.get(key)
            val map1 = map - key
            (map1, entryRef)
          }
        } yield for {
          entryRef <- entryRef
        } yield {
          entryRef.value
        }
      }

      val clear = ref.set(EntryRefs.empty)
    }
  }


  private[scache] def apply[F[_] : Monad, K, V](
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

      val clear = partitions.values.foldMapM(_.clear)
    }
  }


  type EntryRef[F[_], A] = Ref[F, Entry[F, A]]

  implicit class EntryRefOps[F[_], A](val self: EntryRef[F, A]) extends AnyVal {

    def value(implicit F: Monad[F]): F[A] = {
      for {
        entry <- self.get
        value <- entry.value
      } yield value
    }
  }


  implicit class EntryRefOptOps[F[_], A](val self: Option[EntryRef[F, A]]) extends AnyVal {

    def value(implicit F: Monad[F]): F[Option[A]] = self.flatF { _.value }
  }


  type EntryRefs[F[_], K, V] = Map[K, EntryRef[F, V]]

  object EntryRefs {
    def empty[F[_], K, V]: EntryRefs[F, K, V] = Map.empty
  }


  sealed trait Entry[F[_], A] extends Product

  object Entry {

    def loaded[F[_], A](a: A): Entry[F, A] = Loaded(a)

    def loading[F[_], A](deferred: Deferred[F, F[A]]): Entry[F, A] = Loading(deferred)


    final case class Loaded[F[_], A](a: A) extends Entry[F, A]

    final case class Loading[F[_], A](deferred: Deferred[F, F[A]]) extends Entry[F, A]


    implicit class EntryOps[F[_], A](val self: Entry[F, A]) extends AnyVal {

      def value(implicit F: Monad[F]): F[A] = {
        self match {
          case Loaded(a)  => a.pure[F]
          case Loading(a) => a.get.flatten
        }
      }
    }
  }


  implicit class CacheOptionOps[A](val self: Option[A]) extends AnyVal {

    def flatF[F[_], B](f: A => F[B])(implicit F: Monad[F]): F[Option[B]] = {
      self.flatTraverse { a => f(a).map(_.some) }
    }
  }
}