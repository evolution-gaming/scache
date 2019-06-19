package com.evolutiongaming.scache

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, Sync}
import cats.implicits._
import cats.Monad


object LoadingCache {

  private[scache] def of[F[_] : Concurrent, K, V](map: EntryRefs[F, K, V]): F[Cache[F, K, V]] = {
    for {
      ref <- Ref[F].of(map)
    } yield {
      apply(ref)
    }
  }


  private[scache] def apply[F[_] : Concurrent, K, V](
    ref: Ref[F, EntryRefs[F, K, V]]
  ): Cache[F, K, V] = {
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
                  case entry: Entry.Loaded[F, V]  => entry
                  case _    : Entry.Loading[F, V] => Entry.loaded(value)
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


    final case class Loaded[F[_], A](value: A) extends Entry[F, A]

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


  implicit class LoadingCacheOptionOps[A](val self: Option[A]) extends AnyVal {

    def flatF[F[_], B](f: A => F[B])(implicit F: Monad[F]): F[Option[B]] = {
      self.flatTraverse { a => f(a).map(_.some) }
    }
  }
}