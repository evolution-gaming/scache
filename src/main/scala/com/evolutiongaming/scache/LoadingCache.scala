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

      def add = {

        def add(entryRef: EntryRef[F, V], entryRefs: EntryRefs[F, K, V]) = {
          entryRefs
            .get(key)
            .fold {
              (entryRefs.updated(key, entryRef), none[V].pure[F].pure[F])
            } { entryRef =>
              (entryRefs, entryRef.put(loaded))
            }
        }

        for {
          entryRef <- EntryRef.loaded(loaded)
          value    <- ref.modify { entryRefs => add(entryRef, entryRefs) }
          value    <- value
        } yield value
      }

      for {
        entryRefs <- ref.get
        value     <- entryRefs.get(key).fold(add)(_.put(loaded))
      } yield value
    }

    def getOrUpdateReleasable1(key: K)(loaded: => F[EntryRef.Entry.Loaded[F, V]]) = {

      def update = {

        def update(entryRef: EntryRef[F, V], load: F[V]) = {
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

        for {
          entryRefAndLoad  <- EntryRef.loading(loaded, ref.update { _ - key })
          (entryRef, load)  = entryRefAndLoad
          value            <- update(entryRef, load)
        } yield value
      }

      for {
        entryRefs <- ref.get
        value     <- entryRefs.get(key).fold { update } { _.get }
      } yield value
    }

    new Cache[F, K, V] {

      def get(key: K) = {
        ref
          .get
          .flatMap { _.get(key).traverse(_.get) }
          .handleError { _ => none[V] }
      }

      def getOrUpdate(key: K)(value: => F[V]) = {
        getOrUpdateReleasable1(key) {
          for {
            value <- value
          } yield {
            loadedOf(value, none)
          }
        }
      }

      def getOrUpdateReleasable(key: K)(value: => F[Releasable[F, V]]) = {
        getOrUpdateReleasable1(key) {
          for {
            value <- value
          } yield {
            loadedOf(value.value, value.release.some)
          }
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
          entryRefs.map { case (k, v) => k -> v.get }
        }
      }


      def remove(key: K) = {

        def remove(entryRefs: EntryRefs[F, K, V]) = {
          val entryRef = entryRefs.get(key)
          val entryRefs1 = entryRefs.get(key).fold(entryRefs) { _ => entryRefs - key }
          (entryRefs1, entryRef)
        }

        def release(entryRef: EntryRef[F, V]) = {
          val result = for {
            _     <- entryRef.release
            value <- entryRef.get
          } yield {
            value
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
          releases  <- entryRefs.values.toList.foldMapM { _.release.start }
        } yield {
          releases.join
        }
      }
    }
  }


  type EntryRefs[F[_], K, V] = Map[K, EntryRef[F, V]]

  object EntryRefs {
    def empty[F[_], K, V]: EntryRefs[F, K, V] = Map.empty
  }
}