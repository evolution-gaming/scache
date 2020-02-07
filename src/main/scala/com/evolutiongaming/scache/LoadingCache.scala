package com.evolutiongaming.scache

import cats.effect.concurrent.Ref
import cats.effect.implicits._
import cats.effect.{Concurrent, Resource}
import cats.implicits._
import com.evolutiongaming.catshelper.CatsHelper._


object LoadingCache {

  private[scache] def of[F[_] : Concurrent, K, V](
    map: EntryRefs[F, K, V],
  ): Resource[F, Cache[F, K, V]] = {
    for {
      ref   <- Resource.liftF(Ref[F].of(map))
      cache <- of(ref)
    } yield cache
  }


  private[scache] def of[F[_] : Concurrent, K, V](
    ref: Ref[F, EntryRefs[F, K, V]],
  ): Resource[F, Cache[F, K, V]] = {
    Resource.make {
      apply(ref).pure[F]
    } { cache =>
      cache.clear.flatten
    }
  }


  private[scache] def apply[F[_] : Concurrent, K, V](
    ref: Ref[F, EntryRefs[F, K, V]],
  ): Cache[F, K, V] = {

    def loadedOf(value: V, release: Option[F[Unit]]) = {
      EntryRef.Entry.Loaded(
        value,
        release.map(_.handleError(_ => ())))
    }

    def put1(key: K, loaded: EntryRef.Entry.Loaded[F, V]) = {
      ref
        .get
        .flatMap { entryRefs =>
          entryRefs
            .get(key)
            .fold {
              EntryRef
                .loaded(loaded)
                .flatMap { entryRef =>
                  ref
                    .modify { entryRefs =>
                      entryRefs
                        .get(key)
                        .fold {
                          (entryRefs.updated(key, entryRef), none[V].pure[F].pure[F])
                        } { entryRef =>
                          (entryRefs, entryRef.put(loaded))
                        }
                    }
                    .flatten
                }
            } {
              _.put(loaded)
            }
        }
    }

    def getOrUpdateReleasable1(key: K)(loaded: => F[EntryRef.Entry.Loaded[F, V]]) = {
      ref
        .get
        .flatMap { entryRefs =>
          entryRefs
            .get(key)
            .fold {
              EntryRef
                .loading(loaded, ref.update { _ - key })
                .flatMap { case (entryRef, load) =>
                  ref
                    .modify { entryRefs =>
                      entryRefs.get(key).fold {
                        (entryRefs.updated(key, entryRef), load)
                      } { entryRef =>
                        (entryRefs, entryRef.get)
                      }
                    }
                    .flatten
                    .uncancelable
                }
            } {
              _.get
            }
        }
    }

    new Cache[F, K, V] {

      def get(key: K) = {
        ref
          .get
          .flatMap { _.get(key).traverse(_.get) }
          .handleError { _ => none[V] }
      }

      def getOrElse(key: K, default: => V): F[V] = get(key).map(_.getOrElse(default))

      def getOrUpdate(key: K)(value: => F[V]) = {
        getOrUpdateReleasable1(key) {
          value.map { loadedOf(_, none) }
        }
      }

      def getOrUpdateReleasable(key: K)(value: => F[Releasable[F, V]]) = {
        getOrUpdateReleasable1(key) {
          value.map { value => loadedOf(value.value, value.release.some) }
        }
      }


      def put(key: K, value: V) = {
        def loaded = loadedOf(value, none)
        put1(key, loaded)
      }


      def put(key: K, value: V, release: F[Unit]) = {
        def loaded = loadedOf(value, release.some)
        put1(key, loaded)
      }


      val size = {
        ref
          .get
          .map { _.size }
      }


      val keys = {
        ref
          .get
          .map { _.keySet }
      }


      val values = {
        ref
          .get
          .map { _.map { case (k, v) => k -> v.get } }
      }


      def remove(key: K) = {
        ref
          .modify { entryRefs =>
            val entryRef = entryRefs.get(key)
            val entryRefs1 = entryRefs
              .get(key)
              .fold(entryRefs) { _ => entryRefs - key }
            (entryRefs1, entryRef)
          }
          .flatMap { entryRef =>
            entryRef
              .flatTraverse { entryRef =>
                entryRef
                  .release
                  .flatMap { _ => entryRef.get }
                  .redeem((_: Throwable) => none[V], _.some)
              }
              .start
          }
          .uncancelable
          .map { _.join }
      }


      val clear = {
        ref
          .getAndSet(EntryRefs.empty)
          .flatMap { entryRefs =>
            entryRefs
              .values
              .toList
              .foldMapM { _.release.start }
          }
          .uncancelable
          .map { _.join }
      }
    }
  }


  type EntryRefs[F[_], K, V] = Map[K, EntryRef[F, V]]

  object EntryRefs {
    def empty[F[_], K, V]: EntryRefs[F, K, V] = Map.empty
  }
}