package com.evolutiongaming.scache

import cats.effect.{Clock, Ref, Resource, Temporal}
import cats.effect.syntax.all.*
import cats.kernel.CommutativeMonoid
import cats.syntax.all.*
import cats.{Applicative, MonadThrow, Monoid, Parallel}
import com.evolutiongaming.catshelper.ClockHelper.*
import com.evolutiongaming.catshelper.ParallelHelper.*
import com.evolutiongaming.catshelper.Schedule

import scala.concurrent.duration.*

object ExpiringCache {

  type Timestamp = Long

  private[scache] def of[F[_]: Parallel, K, V](
    config: Config[F, K, V]
  )(implicit G: Temporal[F]): Resource[F, Cache[F, K, V]] = {
    
    type E = Entry[V]

    val cooldown           = config.expireAfterRead.toMillis / 5
    val expireAfterReadMs  = config.expireAfterRead.toMillis + cooldown / 2
    val expireAfterWriteMs = config.expireAfterWrite.map { _.toMillis }
    val expireInterval     = {
      val expireInterval = expireAfterWriteMs.fold(expireAfterReadMs) { _ min expireAfterReadMs }
      (expireInterval / 10).millis
    }

    def removeExpiredAndCheckSize(ref: Ref[F, LoadingCache.EntryRefs[F, K, E]], cache: Cache[F, K, E]) = {

      def remove(key: K) = {
        cache
          .remove(key)
          .flatten
          .void
      }

      def removeExpired(key: K, entryRef: LoadingCache.EntryRef[F, Entry[V]]) = {
        entryRef
          .get
          .flatMap { entry =>
            entry.foldMapM { entry =>
              for {
                now               <- Clock[F].millis
                expiredAfterRead   = expireAfterReadMs + entry.value.touched < now
                expiredAfterWrite  = () => expireAfterWriteMs.exists { _ + entry.value.created < now }
                expired            = expiredAfterRead || expiredAfterWrite()
                result            <- if (expired) remove(key) else ().pure[F]
              } yield result
            }
          }
      }

      def notExceedMaxSize(maxSize: Int) = {

        def drop(entryRefs: LoadingCache.EntryRefs[F, K, E]) = {

          final case class Elem(key: K, timestamp: Timestamp)

          val zero = List.empty[Elem]
          entryRefs
            .foldLeft(zero.pure[F]) { case (result, (key, entryRef)) =>
              result.flatMap { result =>
                entryRef
                  .get
                  .map {
                    case Right(a) => Elem(key, a.value.touched) :: result
                    case Left(_)  => result
                  }
              }
            }
            .flatMap { entries =>
              entries
                .sortBy(_.timestamp)
                .take(maxSize / 10)
                .parFoldMap1 { elem => remove(elem.key) }
            }
        }

        for {
          entryRefs <- ref.get
          result    <- if (entryRefs.size > maxSize) drop(entryRefs) else ().pure[F]
        } yield result
      }

      for {
        entryRefs <- ref.get
        result    <- entryRefs.parFoldMap1 { case (key, entryRef) => removeExpired(key, entryRef) }
        _         <- config
          .maxSize
          .foldMapM { maxSize => notExceedMaxSize(maxSize) }
      } yield result
    }

    def refreshEntries(
      refresh: Refresh[K, F[Option[V]]],
      ref: Ref[F, LoadingCache.EntryRefs[F, K, E]],
      cache: Cache[F, K, E]
    ) = {
      ref
        .get
        .flatMap { entryRefs =>
          entryRefs.parFoldMap1 { case (key, entryRef) =>
            entryRef
              .get
              .flatMap { value =>
                value.foldMapM { _ =>
                  refresh
                    .value(key)
                    .flatMap {
                      case Some(value) => entryRef.update1 { _.copy(value = value) }
                      case None        => cache.remove(key).void
                    }
                    .handleError { _ => () }
                }
              }
          }
        }
    }

    def schedule(interval: FiniteDuration)(fa: F[Unit]) = Schedule(interval, interval)(fa)

    val entryRefs = LoadingCache.EntryRefs.empty[F, K, E]
    for {
      ref   <- Ref[F].of(entryRefs).toResource
      cache <- LoadingCache.of(ref)
      _     <- schedule(expireInterval) { removeExpiredAndCheckSize(ref, cache) }
      _     <- config
        .refresh
        .foldMapM { refresh =>
          schedule(refresh.interval) { refreshEntries(refresh, ref, cache) }
        }
    } yield {
      apply(ref, cache, cooldown)
    }
  }


  def apply[F[_] : MonadThrow : Clock, K, V](
    ref: Ref[F, LoadingCache.EntryRefs[F, K, Entry[V]]],
    cache: Cache[F, K, Entry[V]],
    cooldown: Long,
  ): Cache[F, K, V] = {

    type E = Entry[V]

    def entryOf(value: V) = {
      Clock[F]
        .millis
        .map { timestamp =>
          Entry(value, created = timestamp, read = none)
        }
    }

    implicit def monoidUnit: Monoid[F[Unit]] = Applicative.monoid[F, Unit]

    def touch(key: K, entry: E) = {
      for {
        now    <- Clock[F].millis
        result <- if ((entry.touched + cooldown) <= now) {
          ref
            .get
            .flatMap { entries =>
              entries
                .get(key)
                .foldMap { _.update1 { _.touch(now) } }
            }
        } else {
          ().pure[F]
        }
      } yield result
    }

    abstract class ExpiringCache extends Cache.Abstract1[F, K, V]

    new ExpiringCache { self =>

      def get(key: K) = {
        cache
          .get1(key)
          .flatMap {
            case Some(Right(entry)) =>
              touch(key, entry).as {
                entry
                  .value
                  .some
              }
            case Some(Left(entry))  =>
              entry
                .map { _.value.some }
                .handleError { _ => none[V] }
            case None              =>
              none[V].pure[F]
          }
      }

      def get1(key: K) = {
        cache
          .get1(key)
          .flatMap {
            case Some(Right(entry)) =>
              touch(key, entry).as {
                entry
                  .value
                  .asRight[F[V]]
                  .some
              }
            case Some(Left(entry))  =>
              entry
                .map { _.value }
                .asLeft[V]
                .some
                .pure[F]
            case None               =>
              none[Either[F[V], V]].pure[F]
          }
      }

      def getOrUpdate(key: K)(value: => F[V]) = {
        getOrUpdate1(key) { value.map { a => (a, a, none[Release]) } }
          .flatMap {
            case Right(Right(a)) => a.pure[F]
            case Right(Left(a))  => a
            case Left(a)         => a.pure[F]
          }
      }

      def getOrUpdate1[A](key: K)(value: => F[(A, V, Option[Release])]) = {
        cache
          .getOrUpdate1(key) {
            value.flatMap { case (a, value, release) =>
              entryOf(value).map { value => (a, value, release) }
            }
          }
          .flatMap {
            case Right(Right(entry)) =>
              touch(key, entry).as {
                entry
                  .value
                  .asRight[F[V]]
                  .asRight[A]
              }
            case Right(Left(entry))  =>
              entry
                .map { _.value }
                .asLeft[V]
                .asRight[A]
                .pure[F]

            case Left(a) =>
              a
                .asLeft[Either[F[V], V]]
                .pure[F]
          }
      }

      def put(key: K, value: V, release: Option[Release]) = {
        entryOf(value)
          .flatMap { entry =>
            cache
              .put(key, entry, release)
              .map { _.map { _.map { _.value } } }
          }
      }

      def contains(key: K) = cache.contains(key)

      def size = cache.size

      def keys = cache.keys

      def values = {
        cache
          .values
          .map { values =>
            values.map { case (key, entry) =>
              (key, entry.map { _.value })
            }
          }
      }

      def values1 = {
        cache
          .values1
          .map { entries =>
            entries.map { case (key, entry) =>
              val value = entry match {
                case Right(a) => a.value.asRight[F[V]]
                case Left(a)  => a.map { _.value }.asLeft[V]
              }
              (key, value)
            }
          }
      }

      override def readyValues: F[Map[K, V]] = {
        cache
          .readyValues
          .map { values =>
            values.map { case (key, entry) =>
              key -> entry.value
            }
          }
      }

      def remove(key: K) = {
        cache
          .remove(key)
          .map { _.map { _.map { _.value } } }
      }

      def clear = cache.clear

      def foldMap[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = {
        cache.foldMap {
          case (k, Right(v)) => f(k, v.value.asRight)
          case (k, Left(v))  => f(k, v.map { _.value }.asLeft)
        }
      }

      def foldMapPar[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = {
        cache.foldMap {
          case (k, Right(v)) => f(k, v.value.asRight)
          case (k, Left(v))  => f(k, v.map { _.value }.asLeft)
        }
      }
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


  final case class Config[F[_], -K, V](
    expireAfterRead: FiniteDuration,
    expireAfterWrite: Option[FiniteDuration] = None,
    maxSize: Option[Int] = None,
    refresh: Option[Refresh[K, F[Option[V]]]] = None)
}