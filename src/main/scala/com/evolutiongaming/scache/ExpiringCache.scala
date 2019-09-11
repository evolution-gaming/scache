package com.evolutiongaming.scache

import cats.effect.concurrent.Ref
import cats.effect.{Clock, Concurrent, Resource, Timer}
import cats.implicits._
import cats.{Applicative, Monad, Parallel}
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.Schedule

import scala.concurrent.duration._

/**
  * TODO list
  * * add getNotTouch
  * * add getOrUpdateNotTouch
  */
object ExpiringCache {

  type Timestamp = Long

  private[scache] def of[F[_] : Concurrent : Timer : Parallel, K, V](
    expireAfter: FiniteDuration,
    maxSize: Option[Int] = None,
    refresh: Option[Refresh[K, F[V]]] = None,
  ): Resource[F, Cache[F, K, V]] = {

    type E = Entry[V]

    val cooldown       = expireAfter.toMillis / 5
    val expireAfterMs  = expireAfter.toMillis + cooldown / 2
    val expireInterval = (expireAfterMs / 10).millis

    def removeExpiredAndCheckSize(ref: Ref[F, LoadingCache.EntryRefs[F, K, E]]) = {

      def removeExpired(key: K, entryRefs: LoadingCache.EntryRefs[F, K, E]) = {

        def removeExpired(entry: E) = {
          for {
            now    <- Clock[F].millis
            result <- if (entry.timestamp + expireAfterMs < now) ref.update { _ - key } else ().pure[F]
          } yield result
        }

        val entryRef = entryRefs.get(key)
        entryRef.foldMapM { entryRef =>
          for {
            entry  <- entryRef.get
            result <- entry match {
              case entry: LoadingCache.Entry.Loaded[F, E]  => removeExpired(entry.value)
              case _    : LoadingCache.Entry.Loading[F, E] => ().pure[F]
            }
          } yield result
        }
      }

      def notExceedMaxSize(maxSize: Int) = {

        def drop(entryRefs: LoadingCache.EntryRefs[F, K, E]) = {

          case class Elem(key: K, timestamp: Timestamp)

          val zero = List.empty[Elem]
          val entries = entryRefs.foldLeft(zero.pure[F]) { case (result, (key, entryRef)) =>
            for {
              result <- result
              entry  <- entryRef.get
            } yield entry match {
              case entry: LoadingCache.Entry.Loaded[F, E]  => Elem(key, entry.value.timestamp) :: result
              case _    : LoadingCache.Entry.Loading[F, E] => result
            }
          }

          for {
            entries <- entries
            drop     = entries.sortBy(_.timestamp).take(maxSize / 10)
            result  <- drop.foldMapM { elem => ref.update { _ - elem.key } }
          } yield result
        }

        for {
          entryRefs <- ref.get
          result    <- if (entryRefs.size > maxSize) drop(entryRefs) else ().pure[F]
        } yield result
      }

      for {
        entryRefs <- ref.get
        result    <- entryRefs.keys.toList.foldMapM { key => removeExpired(key, entryRefs) }
        _         <- maxSize.foldMapM(notExceedMaxSize)
      } yield result
    }

    def refreshEntries(
      refresh: Refresh[K, F[V]],
      ref: Ref[F, LoadingCache.EntryRefs[F, K, E]]
    ) = {

      def refreshEntry(key: K, entryRef: LoadingCache.EntryRef[F, E]) = {
        val result = for {
          value  <- refresh.value(key)
          result <- entryRef.update {
            case entry: LoadingCache.Entry.Loaded[F, E]  => entry.copy(value = entry.value.copy(value = value))
            case entry: LoadingCache.Entry.Loading[F, E] => entry
          }
        } yield result
        result.handleError { _ => () }
      }

      def refreshEntries(entryRefs: LoadingCache.EntryRefs[F, K, E]) = {
        for {
          key      <- entryRefs.keys.toList
          entryRef <- entryRefs.get(key)
        } yield for {
          entry  <- entryRef.get
          result <- entry match {
            case _: LoadingCache.Entry.Loaded[F, E]  => refreshEntry(key, entryRef)
            case _: LoadingCache.Entry.Loading[F, E] => ().pure[F]
          }
        } yield result
      }

      for {
        entryRefs <- ref.get
        _         <- Parallel.parSequence(refreshEntries(entryRefs))
      } yield {}
    }

    def schedule(interval: FiniteDuration)(fa: F[Unit]) = Schedule(interval, interval)(fa)

    val entryRefs = LoadingCache.EntryRefs.empty[F, K, E]
    val ref = Ref[F].of(entryRefs)
    for {
      ref   <- Resource.liftF(ref)
      cache  = LoadingCache(ref)
      _     <- schedule(expireInterval) { removeExpiredAndCheckSize(ref) }
      _     <- refresh.foldMapM { refresh => schedule(refresh.interval) { refreshEntries(refresh, ref) } }
    } yield {
      apply(ref, cache, cooldown)
    }
  }


  def apply[F[_] : Monad : Clock, K, V](
    ref: Ref[F, LoadingCache.EntryRefs[F, K, Entry[V]]],
    cache: Cache[F, K, Entry[V]],
    cooldown: Long,
  ): Cache[F, K, V] = {

    type E = Entry[V]

    implicit val monoidUnit = Applicative.monoid[F, Unit]

    def touch(key: K, entry: E) = {

      def touch(timestamp: Timestamp): F[Unit] = {

        def touch(entryRef: LoadingCache.EntryRef[F, E]) = {
          entryRef.update {
            case entry: LoadingCache.Entry.Loaded[F, E]  => LoadingCache.Entry.loaded(entry.value.touch(timestamp))
            case entry: LoadingCache.Entry.Loading[F, E] => entry
          }
        }

        for {
          entryRefs <- ref.get
          result    <- entryRefs.get(key).foldMap(touch)
        } yield result
      }

      def shouldTouch(now: Timestamp) = (entry.timestamp + cooldown) <= now

      /*TODO randomize cooldown to avoid contention?*/
      for {
        now    <- Clock[F].millis
        result <- if (shouldTouch(now)) touch(now) else ().pure[F]
      } yield result
    }

    new Cache[F, K, V] {

      def get(key: K) = {
        for {
          entry <- cache.get(key)
          _     <- entry.foldMap { entry => touch(key, entry) }
        } yield for {
          entry <- entry
        } yield {
          entry.value
        }
      }

      def getOrUpdate(key: K)(value: => F[V]) = {

        def entry = {
          for {
            value     <- value
            timestamp <- Clock[F].millis
          } yield {
            Entry(value, timestamp)
          }
        }

        for {
          entry <- cache.getOrUpdate(key)(entry)
          _     <- touch(key, entry)
        } yield {
          entry.value
        }
      }

      def put(key: K, value: V) = {
        for {
          timestamp <- Clock[F].millis
          entry      = Entry(value, timestamp)
          entry     <- cache.put(key, entry)
        } yield for {
          entry <- entry
        } yield for {
          entry <- entry
        } yield {
          entry.value
        }
      }

      def size = cache.size

      def keys = cache.keys

      def values = {
        for {
          entries <- cache.values
        } yield {
          entries.map { case (key, entry) =>
            val value = for {
              entry <- entry
            } yield {
              entry.value
            }
            key -> value
          }
        }
      }

      def remove(key: K) = {
        for {
          entry <- cache.remove(key)
        } yield for {
          entry <- entry
        } yield for {
          entry <- entry
        } yield {
          entry.value
        }
      }

      def clear = cache.clear
    }
  }


  final case class Entry[A](value: A, timestamp: Timestamp) { self =>

    def touch(timestamp: Timestamp): Entry[A] = {
      if (timestamp > self.timestamp) copy(timestamp = timestamp) else self
    }
  }

  
  final case class Refresh[K, V](interval: FiniteDuration, value: K => V)
}