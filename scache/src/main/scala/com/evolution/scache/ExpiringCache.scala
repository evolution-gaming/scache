package com.evolution.scache

import cats.effect.syntax.all.*
import cats.effect.{Async, Clock, Ref, Resource}
import cats.kernel.CommutativeMonoid
import cats.syntax.all.*
import cats.{Applicative, MonadThrow, Monoid}
import com.evolution.scache.Cache.Directive
import com.evolution.scache.LoadingCache.EntryState
import com.evolutiongaming.catshelper.ClockHelper.*
import com.evolutiongaming.catshelper.Schedule

import scala.concurrent.duration.*

object ExpiringCache {

  type Timestamp = Long

  /**
   * Shortest delay the cleanup routine is ever scheduled with, in milliseconds.
   */
  private val MinExpireIntervalMs = 10L

  private[scache] def of[F[_], K, V](
    config: Config[F, K, V],
  )(implicit
    G: Async[F],
  ): Resource[F, Cache[F, K, V]] = {

    type TimestampedValue = Entry[V]

    type LoadingDeferred = LoadingCache.DeferredThrow[F, LoadingCache.Entry[F, TimestampedValue]]

    val cooldown = math.max(config.expireAfterRead.toMillis / 5, 10L)
    val expireAfterReadMs = config.expireAfterRead.toMillis + cooldown / 2
    val expireAfterWriteMs = config.expireAfterWrite.map { _.toMillis }
    val expireAfterMs = expireAfterWriteMs.fold(expireAfterReadMs) { _ min expireAfterReadMs }
    val loadingTimeoutMs = config
      .loadingTimeout
      .fold(expireAfterMs) { _.toMillis }
    /* One cleanup run walks every entry, so the interval is what the cost of the routine is traded
     * against. Values are sampled ten times per expiration, as before, while loads are sampled only
     * twice per `loadingTimeout`, because a load overstaying its welcome by half the timeout is
     * harmless and a short `loadingTimeout` next to a long expiration would otherwise turn the
     * routine into a busy scan of the whole cache. The`MinExpireInterval` keeps a tiny configured duration from
     * scheduling the routine with no delay at all.
     */
    val expireInterval = {
      val interval = (expireAfterMs / 10) min (loadingTimeoutMs / 2)
      (interval max MinExpireInterval).millis
    }

    /* One run of the expiration routine: 
     *  - drops the values that are too old,
     *  - evicts the loads that are taking too long,
     *  - enforces `maxSize`.
     *
     * Loads are expired as well, because a load that never completes would otherwise stay in the
     * map forever, holding the key hostage: nothing can be stored under it, everyone asking for it
     * waits on a `Deferred` that will never complete, and so does the release of the cache itself.
     *
     * The three pieces of state are one and the same map seen from three angles, and are not kept
     * in sync by hand: `entryMap` is the raw per-key state, needed here because the [[Cache]]
     * interface exposes neither the entry states nor the `Deferred` of a load; `cache` is the very
     * same map behind that interface, used for the removals, so that they go through the regular
     * release logic; `loadingSince` is bookkeeping private to this routine, holding the moment each
     * of the currently loading keys was first seen loading, carried over between the runs, as this
     * is the only way to tell how long a load is running. Anything stale in `loadingSince` is
     * ignored and dropped on the next run.
     */
    def removeExpiredAndCheckSize(
      entryMap: LoadingCache.EntryMap[F, K, TimestampedValue],
      cache: Cache[F, K, TimestampedValue],
      loadingSince: Ref[F, Map[K, (LoadingDeferred, Timestamp)]],
    ): F[Unit] = {

      def remove(key: K): F[Unit] = {
        cache
          .remove(key)
          .flatten
          .void
      }

      def removeExpired(key: K, entryRef: LoadingCache.EntryRef[F, TimestampedValue]): F[Unit] = {
        entryRef
          .get
          .flatMap {
            case state: EntryState.Value[F, TimestampedValue] =>
              for {
                now <- Clock[F].millis
                expiredAfterRead = expireAfterReadMs + state.entry.value.touched < now
                expiredAfterWrite = () => expireAfterWriteMs.exists { _ + state.entry.value.created < now }
                expired = expiredAfterRead || expiredAfterWrite()
                result <- if (expired) remove(key) else ().pure[F]
              } yield result
            case _: EntryState.Loading[F, TimestampedValue] => ().pure[F]
            case EntryState.Removed => ().pure[F]
          }
      }

      /* Drops an entry that is still loading, failing everyone waiting for it with `ExpiredError`.
       *
       * Does nothing unless the entry is still loading the very same `deferred`, so that a load
       * that has completed, or has been replaced by a newer one, in the meantime is left alone.
       */
      def evictLoading(
        key: K,
        entryRef: LoadingCache.EntryRef[F, TimestampedValue],
        deferred: LoadingDeferred,
      ): F[Unit] = {
        entryRef
          .modify {
            case state: EntryState.Loading[F, TimestampedValue] if state.deferred == deferred =>
              (EntryState.Removed, true)
            case state =>
              (state, false)
          }
          .flatMap {
            case true =>
              entryMap
                .ref(key)
                .update {
                  case Some(`entryRef`) => none
                  case other => other
                }
                .productR { deferred.complete(ExpiredError.asLeft).void }
            case false =>
              ().pure[F]
          }
          .uncancelable
      }

      /* Evicts the loads that have been running longer than `Config.loadingTimeout`.
       *
       * A load has no timestamp of its own, so its age is counted from the first run of the routine
       * that has seen it, which may be up to one run interval later than the load actually started.
       * The bookkeeping is keyed by the `Deferred` of the load rather than by the key alone, so
       * that a new load of the same key starts its own countdown instead of inheriting the one of
       * its predecessor.
       */
      def removeExpiredLoading(
        loading: List[(K, LoadingCache.EntryRef[F, TimestampedValue], LoadingDeferred)],
      ): F[Unit] = {
        val threshold = loadingTimeoutMs
        for {
          now <- Clock[F].millis
          expired <- loadingSince.modify { seen =>
            val seen1 = loading
              .map { case (key, _, deferred) =>
                val since = seen
                  .get(key)
                  .collect { case (`deferred`, since) => since }
                  .getOrElse(now)
                (key, (deferred, since))
              }
              .toMap
            val expired = loading.filter { case (key, _, deferred) =>
              seen1.get(key).exists { case (deferred1, since) =>
                (deferred1 == deferred) && (since + threshold < now)
              }
            }
            (seen1 -- expired.map { case (key, _, _) => key }, expired)
          }
          result <- expired.foldMapM { case (key, entryRef, deferred) => evictLoading(key, entryRef, deferred) }
        } yield result
      }

      def notExceedMaxSize(maxSize: Int): F[Unit] = {

        def drop(entries: List[(K, LoadingCache.EntryRef[F, TimestampedValue])]): F[Unit] = {

          final case class Elem(key: K, timestamp: Timestamp)

          val zero = List.empty[Elem]
          entries
            .foldLeft(zero.pure[F]) { case (result, (key, entryRef)) =>
              result.flatMap { result =>
                entryRef
                  .get
                  .map {
                    case state: EntryState.Value[F, TimestampedValue] => Elem(key, state.entry.value.touched) :: result
                    case _: EntryState.Loading[F, TimestampedValue] => result
                    case EntryState.Removed => result
                  }
              }
            }
            .flatMap { entries =>
              entries
                .sortBy(_.timestamp)
                .take(maxSize / 10)
                .foldMapM { elem => remove(elem.key) }
            }
        }

        for {
          size <- entryMap.size
          result <- Async[F].whenA(size > maxSize) { entryMap.entries.flatMap(drop) }
        } yield result
      }

      for {
        entries <- entryMap.entries
        result <- entries.foldMapM { case (key, entryRef) => removeExpired(key, entryRef) }
        loading <- entries.foldLeftM(List.empty[(K, LoadingCache.EntryRef[F, TimestampedValue], LoadingDeferred)]) {
          case (acc, (key, entryRef)) =>
            entryRef.get.map {
              case state: EntryState.Loading[F, TimestampedValue] => (key, entryRef, state.deferred) :: acc
              case _ => acc
            }
        }
        _ <- removeExpiredLoading(loading)
        _ <- config
          .maxSize
          .foldMapM { maxSize => notExceedMaxSize(maxSize) }
      } yield result
    }

    def refreshEntries(
      refresh: Refresh[K, F[Option[V]]],
      entryMap: LoadingCache.EntryMap[F, K, TimestampedValue],
      cache: Cache[F, K, TimestampedValue],
    ): F[Unit] = {
      entryMap
        .entries
        .flatMap { entries =>
          entries.foldMapM { case (key, entryRef) =>
            entryRef
              .get
              .flatMap {
                case _: EntryState.Value[F, TimestampedValue] =>
                  refresh
                    .value(key)
                    .flatMap {
                      case Some(value) => entryRef.update1 { _.copy(value = value) }
                      case None => cache.remove(key).void
                    }
                    .handleError { _ => () }
                case _: EntryState.Loading[F, TimestampedValue] => ().pure[F]
                case EntryState.Removed => ().pure[F]
              }
          }
        }
    }

    def schedule(interval: FiniteDuration)(fa: F[Unit]): Resource[F, Unit] = Schedule(interval, interval)(fa)

    for {
      entryMap <- LoadingCache.EntryMap.of[F, K, TimestampedValue].toResource
      loadingSince <- Ref[F].of(Map.empty[K, (LoadingDeferred, Timestamp)]).toResource
      cache <- LoadingCache.of(entryMap)
      _ <- schedule(expireInterval) { removeExpiredAndCheckSize(entryMap, cache, loadingSince) }
      _ <- config
        .refresh
        .foldMapM { refresh =>
          schedule(refresh.interval) { refreshEntries(refresh, entryMap, cache) }
        }
    } yield {
      apply(entryMap, cache, cooldown)
    }
  }

  def apply[F[_]: MonadThrow: Clock, K, V](
    entryMap: LoadingCache.EntryMap[F, K, Entry[V]],
    cache: Cache[F, K, Entry[V]],
    cooldown: Long,
  ): Cache[F, K, V] = {

    type TimestampedValue = Entry[V]

    def entryOf(value: V): F[TimestampedValue] = {
      Clock[F]
        .millis
        .map { timestamp =>
          Entry(value, created = timestamp, read = none)
        }
    }

    implicit def monoidUnit: Monoid[F[Unit]] = Applicative.monoid[F, Unit]

    def touch(key: K, entry: TimestampedValue): F[Unit] = {
      for {
        now <- Clock[F].millis
        result <- if ((entry.touched + cooldown) <= now) {
          entryMap
            .lookup(key)
            .flatMap { _.foldMap { _.update1 { _.touch(now) } } }
        } else {
          ().pure[F]
        }
      } yield result
    }

    abstract class ExpiringCache extends Cache.Abstract1[F, K, V]

    new ExpiringCache { self =>
      def get(key: K): F[Option[V]] = {
        cache
          .get1(key)
          .flatMap {
            case Some(Right(entry)) =>
              touch(key, entry).as {
                entry
                  .value
                  .some
              }
            case Some(Left(entry)) =>
              entry
                .map { _.value.some }
                .handleError { _ => none[V] }
            case None =>
              none[V].pure[F]
          }
      }

      def get1(key: K): F[Option[Either[F[V], V]]] = {
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
            case Some(Left(entry)) =>
              entry
                .map { _.value }
                .asLeft[V]
                .some
                .pure[F]
            case None =>
              none[Either[F[V], V]].pure[F]
          }
      }

      def getOrUpdate(key: K)(value: => F[V]): F[V] = {
        getOrUpdate1(key) { value.map { a => (a, a, none[Release]) } }
          .flatMap {
            case Right(Right(a)) => a.pure[F]
            case Right(Left(a)) => a
            case Left(a) => a.pure[F]
          }
      }

      def getOrUpdate1[A](key: K)(value: => F[(A, V, Option[Release])]): F[Either[A, Either[F[V], V]]] = {
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
            case Right(Left(entry)) =>
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

      def put(key: K, value: V, release: Option[Release]): F[F[Option[V]]] = {
        entryOf(value)
          .flatMap { entry =>
            cache
              .put(key, entry, release)
              .map { _.map { _.map { _.value } } }
          }
      }

      // Modifying existing entry creates a new one, since the old one will be released.
      def modify[A](key: K)(f: Option[V] => (A, Directive[F, V])): F[(A, Option[F[Unit]])] =
        Clock[F]
          .millis
          .flatMap { timestamp =>
            val adaptedF: Option[Entry[V]] => (A, Directive[F, Entry[V]]) = entry =>
              f(entry.map(_.value)) match {
                case (a, put: Directive.Put[F, V]) =>
                  (a, Directive.Put(Entry(put.value, timestamp, none), put.release))
                case (a, Directive.Ignore) => (a, Directive.Ignore)
                case (a, Directive.Remove) => (a, Directive.Remove)
              }
            cache.modify(key)(adaptedF)
          }

      def contains(key: K): F[Boolean] = cache.contains(key)

      def size: F[Int] = cache.size

      def keys: F[Set[K]] = cache.keys

      def values: F[Map[K, F[V]]] = {
        cache
          .values
          .map { values =>
            values.map { case (key, entry) =>
              (key, entry.map { _.value })
            }
          }
      }

      def values1: F[Map[K, Either[F[V], V]]] = {
        cache
          .values1
          .map { entries =>
            entries.map { case (key, entry) =>
              val value = entry match {
                case Right(a) => a.value.asRight[F[V]]
                case Left(a) => a.map { _.value }.asLeft[V]
              }
              (key, value)
            }
          }
      }

      def remove(key: K): F[F[Option[V]]] = {
        cache
          .remove(key)
          .map { _.map { _.map { _.value } } }
      }

      def clear: F[F[Unit]] = cache.clear

      def foldMap[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]): F[A] = {
        cache.foldMap {
          case (k, Right(v)) => f(k, v.value.asRight)
          case (k, Left(v)) => f(k, v.map { _.value }.asLeft)
        }
      }

      def foldMapPar[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]): F[A] = {
        cache.foldMap {
          case (k, Right(v)) => f(k, v.value.asRight)
          case (k, Left(v)) => f(k, v.map { _.value }.asLeft)
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

  /**
   * Configuration of a refresh background job.
   *
   * Usage example (`SettingService.get` returns `F[Option[Setting]]`):
   * {{{
   * ExpiringCache.Refresh(
   *   interval = 1.minute,
   *   value = key => SettingService.getOrNone(key)
   * )
   * }}}
   *
   * @param interval
   *   How often the refresh routine should be called. Note, that all cache entries will be
   *   refreshed regardless how long ago these were added to the cache, hence the operation might be
   *   expensive.
   * @param value
   *   The function which returns a value for the specific key. While the function itself is pure,
   *   all the current implementation use `Refresh[K, F[Option[T]]]`, so `V` is not a real value,
   *   but an effectful function which calculates a value. The [[scala.Option]] is used to indicate
   *   if value should be removed (i.e. [[scala.None]] means the key is to be deleted).
   */
  final case class Refresh[-K, +V](interval: FiniteDuration, value: K => V)

  object Refresh {
    def apply[K](interval: FiniteDuration): Apply[K] = new Apply(interval)

    private[Refresh] final class Apply[K](val interval: FiniteDuration) extends AnyVal {

      def apply[V](f: K => V): Refresh[K, V] = Refresh(interval, f)
    }
  }

  /**
   * Configuration of expiring cache, including the potential refresh routine.
   *
   * Performance consideration: The frequency of internal expiration routine depends on
   * `expireAfterRead` and `expireAfterWrite` parameters (it is actually done more often, for sake
   * of faster cleanup), so the very small value set for any of these parameters may affect the
   * performance of the cache, as cleanup will happen too often.
   *
   * Usage example (`SettingService.get` returns `F[Option[Setting]]`):
   * {{{
   * ExpiringCache.Config(
   *   expireAfterRead = 1.minute,
   *   expireAfterWrite = None,
   *   maxSize = None,
   *   refresh = Some(ExpiringCache.Refresh(
   *     interval = 1.minute,
   *     value = key => SettingService.get(key)
   *   ))
   * }}}
   *
   * @param expireAfterRead
   *   The value will be removed after the period set by this parameter if it was not read (i.e. one
   *   of methods reading the value such as [[Cache#get]] or [[Cache#getOrUpdate]] method was not
   *   called). Note, that this removal has a best effort guarantee, i.e. there is possibility that
   *   value is still there after it expires.
   * @param expireAfterWrite
   *   If set to [[scala.Some]], the value will be removed after the period set by this parameter
   *   regardless if it was touched by [[Cache#get]] or similar methods. Note, that this removal has
   *   a best effort guarantee, i.e. there is possibility that value is still there after it
   *   expires.
   * @param maxSize
   *   If set then the cache implementation will try to keep the cache size under `maxSize` whenever
   *   clean up routine happens. If the cache size exceeds the value, it will try to drop part of
   *   non-expired element sorted by the timestamp, when these elements were last read. There is no
   *   guarantee, though, that this size will not be exceeded a bit, if a lot of elements are put
   *   into cache between the cleanup calls.
   * @param refresh
   *   If set to [[scala.Some]], the cache will schedule a background job, which will refresh or
   *   remove the _existing_ values regularly. The keys not already present in a cache will not be
   *   affected anyhow. See [[Refresh]] documentation for more details.
   * @param loadingTimeout
   *   How long a value computation started by [[Cache#getOrUpdate]] is allowed to run before the
   *   entry is evicted and everyone waiting for it fails with [[ExpiredError]]. Without it a
   *   computation that never completes would hold the key forever. If set to [[scala.None]], the
   *   smaller of `expireAfterRead` and `expireAfterWrite` is used. Note, that the load is not
   *   cancelled, only detached from the cache, and that this, too, is best effort: the eviction
   *   only happens on a cleanup run, so a load may outlive the timeout by up to one run interval.
   */
  final case class Config[F[_], -K, V](
    expireAfterRead: FiniteDuration,
    expireAfterWrite: Option[FiniteDuration] = None,
    maxSize: Option[Int] = None,
    refresh: Option[Refresh[K, F[Option[V]]]] = None,
    loadingTimeout: Option[FiniteDuration] = None,
  )

}
