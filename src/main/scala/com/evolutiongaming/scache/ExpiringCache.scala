package com.evolutiongaming.scache

import cats.effect.concurrent.Ref
import cats.effect.{Clock, Concurrent, Resource, Timer}
import cats.syntax.all._
import cats.{Applicative, Monad, Parallel}
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.ParallelHelper._
import com.evolutiongaming.catshelper.Schedule

import scala.concurrent.duration._

object ExpiringCache {

  type Timestamp = Long

  private[scache] def of[F[_] : Concurrent : Timer : Parallel, K, V](
    config: Config[F, K, V]
  ): Resource[F, Cache[F, K, V]] = {
    
    type E = Entry[V]

    val cooldown           = config.expireAfterRead.toMillis / 5
    val expireAfterReadMs  = config.expireAfterRead.toMillis + cooldown / 2
    val expireAfterWriteMs = config.expireAfterWrite.map { _.toMillis }
    val expireInterval     = {
      val expireInterval = expireAfterWriteMs.fold(expireAfterReadMs) { _ min expireAfterReadMs }
      (expireInterval / 10).millis
    }

    def removeExpiredAndCheckSize(ref: Ref[F, LoadingCache.EntryRefs[F, K, E]], cache: Cache[F, K, E]) = {

      def remove(key: K) = cache.remove(key).flatten.void

      def removeExpired(key: K, entryRefs: LoadingCache.EntryRefs[F, K, E]) = {

        def removeExpired(entry: E) = {
          for {
            now               <- Clock[F].millis
            expiredAfterRead   = expireAfterReadMs + entry.touched < now
            expiredAfterWrite  = () => expireAfterWriteMs.exists { _ + entry.created < now }
            expired            = expiredAfterRead || expiredAfterWrite()
            result            <- if (expired) remove(key) else ().pure[F]
          } yield result
        }

        val entryRef = entryRefs.get(key)
        entryRef.foldMapM { entryRef =>
          for {
            values <- entryRef.getLoaded
            result <- values.parFoldMap(removeExpired)
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
              value  <- entryRef.getLoaded
            } yield {
              value.fold {
                result
              } { value =>
                Elem(key, value.touched) :: result
              }
            }
          }

          for {
            entries <- entries
            drop     = entries.sortBy(_.timestamp).take(maxSize / 10)
            result  <- drop.parFoldMap { elem => remove(elem.key) }
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
        _         <- config.maxSize.foldMapM(notExceedMaxSize)
      } yield result
    }

    def refreshEntries(
      refresh: Refresh[K, F[Option[V]]],
      ref: Ref[F, LoadingCache.EntryRefs[F, K, E]],
      cache: Cache[F, K, E]
    ) = {

      def refreshEntry(key: K, entryRef: EntryRef[F, E]) = {
        val result = for {
          value  <- refresh.value(key)
          result <- value match {
            case Some(value) => entryRef.updateLoaded(_.copy(value = value))
            case None        => cache.remove(key).void
          }
        } yield result
        result.handleError { _ => () }
      }

      def refreshEntries(entryRefs: LoadingCache.EntryRefs[F, K, E]) = {
        for {
          key      <- entryRefs.keys.toList
          entryRef <- entryRefs.get(key)
        } yield for {
          value  <- entryRef.getLoaded
          result <- value.foldMapM { _ => refreshEntry(key, entryRef) }
        } yield result
      }

      for {
        entryRefs <- ref.get
        _         <- refreshEntries(entryRefs).parSequence
      } yield {}
    }

    def schedule(interval: FiniteDuration)(fa: F[Unit]) = Schedule(interval, interval)(fa)

    val entryRefs = LoadingCache.EntryRefs.empty[F, K, E]
    val ref = Ref[F].of(entryRefs)

    for {
      ref   <- Resource.liftF(ref)
      cache <- LoadingCache.of(ref)
      _     <- schedule(expireInterval) { removeExpiredAndCheckSize(ref, cache) }
      _     <- config.refresh.foldMapM { refresh => schedule(refresh.interval) { refreshEntries(refresh, ref, cache) } }
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

    def entryOf(value: V) = {
      for {
        timestamp <- Clock[F].millis
      } yield {
        Entry(value,  created = timestamp, read = none)
      }
    }

    implicit val monoidUnit = Applicative.monoid[F, Unit]

    def touch(key: K, entry: E) = {

      def touch(timestamp: Timestamp): F[Unit] = {

        def touch(entryRef: EntryRef[F, E]) = {
          entryRef.updateLoaded(_.touch(timestamp))
        }

        for {
          entryRefs <- ref.get
          result    <- entryRefs.get(key).foldMap(touch)
        } yield result
      }

      def shouldTouch(now: Timestamp) = (entry.touched + cooldown) <= now

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

      def getOrElse(key: K, default: => F[V]) = {
        for {
          stored <- get(key)
          result <- stored.fold(default)(_.pure[F])
        } yield {
          result
        }
      }

      def getOrUpdate(key: K)(value: => F[V]) = {

        def entry = {
          for {
            value <- value
            entry <- entryOf(value)
          } yield entry
        }

        for {
          entry <- cache.getOrUpdate(key)(entry)
          _     <- touch(key, entry)
        } yield {
          entry.value
        }
      }

      def getOrUpdateOpt(key: K)(value: => F[Option[V]]) = {
        def entry = {
          for {
            value <- value
            value <- value.traverse { value => entryOf(value) }
          } yield value
        }

        for {
          entry <- cache.getOrUpdateOpt(key)(entry)
          _     <- entry.traverse { entry => touch(key, entry) }
        } yield for {
          entry <- entry
        } yield {
          entry.value
        }
      }

      def getOrUpdateReleasable(key: K)(value: => F[Releasable[F, V]]) = {

        def entry = {
          for {
            value <- value
            entry <- entryOf(value.value)
          } yield {
            value.copy(value = entry)
          }
        }

        for {
          entry <- cache.getOrUpdateReleasable(key)(entry)
          _     <- touch(key, entry)
        } yield {
          entry.value
        }
      }

      def getOrUpdateReleasableOpt(key: K)(value: => F[Option[Releasable[F, V]]]) = {
        def entry = {
          for {
            value <- value
            value <- value.traverse { value =>
              for {
                entry <- entryOf(value.value)
              } yield {
                value.copy(value = entry)
              }
            }
          } yield value
        }

        for {
          entry <- cache.getOrUpdateReleasableOpt(key)(entry)
          _     <- entry.traverse { entry => touch(key, entry) }
        } yield for {
          entry <- entry
        } yield {
          entry.value
        }
      }

      def put(key: K, value: V) = {
        for {
          entry <- entryOf(value)
          entry <- cache.put(key, entry)
        } yield for {
          entry <- entry
        } yield for {
          entry <- entry
        } yield {
          entry.value
        }
      }

      def put(key: K, value: V, release: F[Unit]) = {
        for {
          entry <- entryOf(value)
          entry <- cache.put(key, entry, release)
        } yield for {
          entry <- entry
        } yield for {
          entry <- entry
        } yield {
          entry.value
        }
      }

      def contains(key: K) = cache.contains(key)

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


  final case class Entry[A](value: A, created: Timestamp, read: Option[Timestamp]) { self =>

    def touch(timestamp: Timestamp): Entry[A] = {
      if (self.read.forall { timestamp > _ }) copy(read = timestamp.some)
      else self
    }

    def touched: Timestamp = read.getOrElse(created)
  }

  
  final case class Refresh[-K, +V](interval: FiniteDuration, value: K => V)

  object Refresh {
    def apply[K](interval: FiniteDuration): Apply[K] = new Apply(interval)

    private[Refresh] final class Apply[K](val interval: FiniteDuration) extends AnyVal {

      def apply[V](f: K => V): Refresh[K, V] = Refresh(interval, f)
    }
  }


  case class Config[F[_], -K, V](
    expireAfterRead: FiniteDuration,
    expireAfterWrite: Option[FiniteDuration] = none,
    maxSize: Option[Int] = none,
    refresh: Option[Refresh[K, F[Option[V]]]] = none)
}