package com.evolutiongaming.scache

import cats.Monad
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.implicits._
import cats.effect.Concurrent
import cats.implicits._
import com.evolutiongaming.catshelper.CatsHelper._


object LoadingCache {

  private[scache] def of[F[_] : Concurrent, K, V](
    map: EntryRefs[F, K, V],
  ): F[Cache[F, K, V]] = {
    for {
      ref <- Ref[F].of(map)
    } yield {
      apply(ref)
    }
  }


  private[scache] def apply[F[_] : Concurrent, K, V](
    ref: Ref[F, EntryRefs[F, K, V]],
  ): Cache[F, K, V] = {

    new Cache[F, K, V] {

      def get(key: K) = {
        ref
          .get
          .flatMap { _.get(key).traverse(_.value) }
          .handleError { _ => none[V] }
      }

      def getOrUpdate(key: K)(value: => F[V]) = {
        getOrUpdateReleasable(key) { value.map { Releasable(_, ().pure[F]) } }
      }

      def getOrUpdateReleasable(key: K)(value: => F[Releasable[F, V]]) = {

        def update = {

          def loadOf(entryRef: EntryRef[F, V], deferred: Deferred[F, F[Entry.Loaded[F, V]]]) = {

            def update(releasable: Releasable[F, V]) = {
              val release = releasable.release.start.void
              val loaded = Entry.Loaded(releasable.value, release)
              entryRef
                .modify {
                  case entry: Entry.Loaded[F, V]  => (entry, loaded.release as entry.asRight[Throwable])
                  case _    : Entry.Loading[F, V] => (loaded, loaded.asRight[Throwable].pure[F])
                }
                .flatten
            }

            def remove(error: Throwable) = {
              for {
                entry <- entryRef.get
                entry <- entry match {
                  case entry: Entry.Loaded[F, V]  => entry.asRight[Throwable].pure[F]
                  case _    : Entry.Loading[F, V] => ref.update { _ - key } as error.asLeft[Entry.Loaded[F, V]]
                }
              } yield entry
            }

            val load = for {
              value <- value.attempt
              entry <- value.fold(remove, update)
              _     <- deferred.complete(entry.liftTo[F]).handleError { _ => () }
            } yield {}

            for {
              _     <- load.start
              value <- deferred.get
              value <- value
            } yield value
          }

          def update(entryRef: EntryRef[F, V], load: F[Entry.Loaded[F, V]]) = {
            ref
              .modify { entryRefs =>
                entryRefs.get(key).fold {
                  (entryRefs.updated(key, entryRef), load)
                } { entryRef =>
                  (entryRefs, entryRef.loaded)
                }
              }
              .flatten
              .uncancelable
          }

          for {
            deferred <- Deferred[F, F[Entry.Loaded[F, V]]]
            entryRef <- Ref[F].of(Entry.loading(deferred))
            load      = loadOf(entryRef, deferred)
            value    <- update(entryRef, load)
          } yield value
        }

        for {
          entryRefs <- ref.get
          value     <- entryRefs.get(key).fold { update } { _.loaded }
        } yield value.value
      }


      def put(key: K, value: V) = {
        put(key, value, ().pure[F])
      }


      def put(key: K, value: V, release: F[Unit]) = {
        val loaded = Entry.Loaded[F, V](value, release)

        def update(entryRef: EntryRef[F, V]) = {

          def onLoaded(entry: Entry.Loaded[F, V]) = entry.release.start as entry.value.some

          def onLoading(deferred: Deferred[F, F[Entry.Loaded[F, V]]]) = {

            def onConflict: F[Option[V]] = {
              for {
                loaded <- deferred.get
                value  <- loaded.redeemWith((_: Throwable) => none[V].pure[F], onLoaded)
              } yield value
            }

            deferred
              .complete(loaded.pure[F])
              .redeemWith((_: Throwable) => onConflict, _ => none[V].pure[F])
          }

          entryRef
            .getAndSet(loaded)
            .flatMap {
              case entry: Entry.Loaded[F, V]  => onLoaded(entry)
              case entry: Entry.Loading[F, V] => onLoading(entry.deferred)
            }
            .uncancelable
        }

        def add = {

          def add(entryRef: EntryRef[F, V], entryRefs: EntryRefs[F, K, V]) = {
            entryRefs
              .get(key)
              .fold {
                (entryRefs.updated(key, entryRef), none[V].pure[F])
              } { entryRef =>
                (entryRefs, update(entryRef))
              }
          }

          for {
            entryRef <- Ref.of[F, Entry[F, V]](loaded)
            value    <- ref.modify { entryRefs => add(entryRef, entryRefs) }
            value    <- value
          } yield value
        }

        for {
          entryRefs <- ref.get
          value     <- entryRefs.get(key).fold(add)(update)
        } yield value
      }


      val size = {
        for {
          entryRefs <- ref.get
        } yield {
          entryRefs.size
        }
      }


      val keys = {
        for {
          entryRefs <- ref.get
        } yield {
          entryRefs.keySet
        }
      }


      val values = {
        for {
          entryRefs <- ref.get
        } yield {
          entryRefs.map { case (k, v) => k -> v.value }
        }
      }


      def remove(key: K) = {

        def remove(entryRefs: EntryRefs[F, K, V]) = {
          val entryRef = entryRefs.get(key)
          val entryRefs1 = entryRef.fold(entryRefs) { _ => entryRefs - key }
          (entryRefs1, entryRef)
        }

        def release(entryRef: EntryRef[F, V]) = {
          val result = for {
            loaded <- entryRef.loaded
            _      <- loaded.release
          } yield {
            loaded.value
          }
          result.redeem((_ : Throwable) => none[V], _.some)
        }

        val result = for {
          entryRef <- ref.modify(remove)
          value    <- entryRef.flatTraverse(release).start
        } yield {
          value.join
        }
        result.uncancelable
      }


      val clear = {
        for {
          entryRefs <- ref.getAndSet(EntryRefs.empty)
          _         <- entryRefs.values.toList.foldMapM { _.release.start }
        } yield {}
      }
    }
  }


  type EntryRef[F[_], A] = Ref[F, Entry[F, A]]

  implicit class EntryRefOps[F[_], A](val self: EntryRef[F, A]) extends AnyVal {

    def value(implicit F: Monad[F]): F[A] = loaded.map(_.value)

    def loaded(implicit F: Monad[F]): F[Entry.Loaded[F, A]] = self.get.flatMap(_.loaded)

    def release(implicit F: Monad[F]): F[Unit] = loaded.flatMap(_.release)
  }


  type EntryRefs[F[_], K, V] = Map[K, EntryRef[F, V]]

  object EntryRefs {
    def empty[F[_], K, V]: EntryRefs[F, K, V] = Map.empty
  }


  sealed trait Entry[F[_], A] extends Product

  object Entry {

    def loaded[F[_], A](value: A, release: F[Unit]): Entry[F, A] = {
      Loaded(value, release)
    }

    def loading[F[_], A](deferred: Deferred[F, F[Loaded[F, A]]]): Entry[F, A] = {
      Loading(deferred)
    }


    final case class Loaded[F[_], A](value: A, release: F[Unit]) extends Entry[F, A]

    final case class Loading[F[_], A](deferred: Deferred[F, F[Loaded[F, A]]]) extends Entry[F, A]


    implicit class EntryOpsLoadingCache[F[_], A](val self: Entry[F, A]) extends AnyVal {

      def loaded(implicit F: Monad[F]): F[Loaded[F, A]] = {
        self match {
          case a: Loaded[F, A] => a.pure[F]
          case Loading(a)      => a.get.flatten
        }
      }
    }
  }
}