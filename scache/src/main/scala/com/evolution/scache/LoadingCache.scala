package com.evolution.scache

import cats.effect.*
import cats.effect.implicits.*
import cats.effect.std.MapRef
import cats.kernel.CommutativeMonoid
import cats.syntax.all.*
import cats.{Applicative, Functor, Monad, MonadThrow, Parallel}
import com.evolution.scache.Cache.Directive
import com.evolutiongaming.catshelper.ParallelHelper.*

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*

/**
 * Cache able to load values, i.e. to deduplicate concurrent computations of the same key.
 *
 * =State=
 *
 * The state is kept on two levels:
 *   - the outer level, [[LoadingCache.EntryMap]], answers "is there an entry for this key?" and is
 *     backed by a [[java.util.concurrent.ConcurrentHashMap]] exposed as a per-key
 *     [[cats.effect.std.MapRef]];
 *   - the inner level, [[LoadingCache.EntryRef]], answers "what happened to the value of this
 *     entry?" and is a `Ref` holding an [[LoadingCache.EntryState]].
 *
 * A key is in the cache if, and only if, the outer level holds an `EntryRef` for it and that
 * `EntryRef` is not in [[LoadingCache.EntryState.Removed]] state.
 *
 * =Why a ConcurrentHashMap=
 *
 * The outer level used to be a single `Ref[F, Map[K, EntryRef]]`, so every insertion or removal of
 * any key had to CAS one and the same `Ref`. That had two consequences:
 *   - operations on unrelated keys invalidated each other, so a `getOrUpdate` of one key could be
 *     starved by a steady stream of writes of other keys, and the retry limit guarding that loop
 *     turned such contention into a failure;
 *   - every write copied the entire map.
 *
 * With `MapRef.fromConcurrentHashMap` a CAS is scoped to a single key: operations on distinct keys
 * never contend, and no retry limit is needed, because the loops below only spin on a real race
 * over the same key, and every such race is won by a fiber that makes progress. Enumeration
 * (`keys`, `entries`, `size`) is served by the `ConcurrentHashMap` itself, i.e. it is a weakly
 * consistent view rather than an atomic snapshot.
 *
 * =Entry lifecycle=
 *
 * `getOrUpdate` installs an entry in `Loading` state, holding a `Deferred` that every other fiber
 * asking for the same key awaits, and only then computes the value. The load then either
 *   - stores the computed value, moving the entry to `Value` state, or
 *   - drops the entry from the map and propagates the error to the caller and to the waiters, if
 *     the computation failed, or
 *   - discards and releases its own result, if `put` or `modify` stored another value under the key
 *     meanwhile, in which case the value of the winner is returned to the caller, or
 *   - discards and releases its own result, if the load was cancelled.
 *
 * Neither `remove` nor `clear` cancels a load in flight, so a load that outlives one of them still
 * has a value on its hands. After a `remove` it stores that value under the key again, putting the
 * key back into the cache; after a `clear` it stores it into the entry the `clear` has already
 * unlinked, and the `clear` is the one that awaits and releases it.
 *
 * `Removed` is a tombstone meaning "this `EntryRef` is no longer in the map, look the key up
 * again". It is needed because the two levels cannot be updated atomically together, so a fiber
 * that looked an `EntryRef` up earlier needs a way to notice that its reference went stale. All
 * retry loops here are driven by it: seeing `Removed` means re-reading the key, and the fiber that
 * installed the tombstone is already committed to unlinking that key, hence the loops terminate.
 *
 * =Releasing values=
 *
 * A value is released exactly once, by the fiber that took it out of the entry, i.e. replaced or
 * removed it. A fiber whose computed value did not make it into the map releases it itself. To
 * avoid making unrelated callers wait for a foreign `release`, releases of values the caller did
 * not ask about are started in the background.
 *
 * =Cancellation=
 *
 * Only the user-supplied computation is cancelable, all state transitions are masked. Cancelling a
 * load flips its own `Loading` state to `Removed`, unlinks the key, always completes the `Deferred`
 * with [[CancelledError]] so that waiters fail instead of hanging, even if the entry had already
 * been taken away by `remove`, and releases the value if the computation did manage to produce one.
 * Without that cleanup a cancelled load would leave behind a `Loading` entry with a `Deferred`
 * nobody is going to complete, which makes the key unusable forever and blocks the waiters,
 * `clear`, and therefore the release of the cache itself.
 */
private[scache] object LoadingCache {

  def of[F[_]: Async, K, V]: Resource[F, Cache[F, K, V]] = {
    for {
      entryMap <- EntryMap.of[F, K, V].toResource
      cache <- of(entryMap)
    } yield cache
  }

  /**
   * Cache over an existing [[EntryMap]], clearing it, and thus releasing all the values, when the
   * resource is released.
   */
  def of[F[_]: Async, K, V](
    entryMap: EntryMap[F, K, V],
  ): Resource[F, Cache[F, K, V]] = {
    Resource.make {
      apply(entryMap).pure[F]
    } { cache =>
      cache.clear.flatten
    }
  }

  /**
   * Per-key view over the cache state: mutations go through [[cats.effect.std.MapRef]], so
   * operations on distinct keys never contend, while enumeration is served by the backing
   * [[java.util.concurrent.ConcurrentHashMap]].
   *
   * Exposed as a trait rather than used directly, because [[ExpiringCache]] needs to walk and evict
   * entries behind the back of the [[Cache]] interface, and because it makes the contention
   * behaviour testable.
   */
  trait EntryMap[F[_], K, V] {

    /**
     * Atomic per-key handle on the map, where `None` stands for "no entry for this key": setting it
     * to `None` removes the key, setting it to `Some` inserts or replaces the entry. This is the
     * only way the mapping itself is modified, and the CAS it performs is scoped to `key`.
     */
    def ref(key: K): Ref[F, Option[EntryRef[F, V]]]

    /**
     * Non-atomic read of the entry, used when the mapping is not going to be modified.
     */
    def lookup(key: K): F[Option[EntryRef[F, V]]]

    /**
     * Weakly consistent view of the keys, i.e. concurrent modifications may or may not be seen.
     */
    def keys: F[Set[K]]

    /**
     * Weakly consistent view of the entries, see [[keys]].
     */
    def entries: F[List[(K, EntryRef[F, V])]]

    /**
     * Number of entries, including the ones being loaded or removed, hence an upper bound of the
     * number of values available.
     */
    def size: F[Int]

    def contains(key: K): F[Boolean]
  }

  object EntryMap {

    def of[F[_]: Sync, K, V]: F[EntryMap[F, K, V]] = {
      Sync[F]
        .delay { new ConcurrentHashMap[K, EntryRef[F, V]]() }
        .map { chm => apply(chm) }
    }

    /**
     * Built over an explicitly passed [[java.util.concurrent.ConcurrentHashMap]] rather than via
     * `MapRef.ofConcurrentHashMap`, because the latter only hands out the per-key `Ref`s, while
     * [[EntryMap.keys]], [[EntryMap.entries]], [[EntryMap.size]] and [[EntryMap.contains]] need the
     * map itself.
     */
    def apply[F[_]: Sync, K, V](chm: ConcurrentHashMap[K, EntryRef[F, V]]): EntryMap[F, K, V] = {
      val mapRef = MapRef.fromConcurrentHashMap[F, K, EntryRef[F, V]](chm)
      new EntryMap[F, K, V] {

        def ref(key: K): Ref[F, Option[EntryRef[F, V]]] =
          mapRef(key)

        def lookup(key: K): F[Option[EntryRef[F, V]]] =
          Sync[F].delay { Option(chm.get(key)) }

        def keys: F[Set[K]] =
          Sync[F].delay { chm.keySet().asScala.toSet }

        def entries: F[List[(K, EntryRef[F, V])]] =
          Sync[F].delay {
            chm
              .entrySet()
              .iterator()
              .asScala
              .map { entry => (entry.getKey, entry.getValue) }
              .toList
          }

        def size: F[Int] =
          Sync[F].delay { chm.mappingCount().toInt }

        def contains(key: K): F[Boolean] =
          Sync[F].delay { chm.containsKey(key) }
      }
    }
  }

  /**
   * Cache over an existing [[EntryMap]], which never releases the values it still holds, hence
   * meant to be wrapped into a resource by [[of]] rather than used directly.
   */
  def apply[F[_]: Async, K, V](
    entryMap: EntryMap[F, K, V],
  ): Cache[F, K, V] = {

    val F = Async[F]

    val handleReleaseError = (e: Throwable) => {
      System.err.println(s"scache: failed to release cache entry: $e")
    }

    def entryOf(value: V, release: Option[F[Unit]]): Entry[F, V] = {
      Entry(
        value = value,
        release = release.map { _.handleError(handleReleaseError) },
      )
    }

    abstract class LoadingCache extends Cache.Abstract1[F, K, V]

    new LoadingCache {

      def get(key: K): F[Option[V]] = {
        entryMap
          .lookup(key)
          .flatMap {
            _.fold {
              none[V].pure[F]
            } { entryRef =>
              entryRef
                .get
                .flatMap {
                  case state: EntryState.Value[F, V] =>
                    state
                      .entry
                      .value
                      .some
                      .pure[F]
                  case state: EntryState.Loading[F, V] =>
                    state
                      .deferred
                      .get
                      .map { entry =>
                        entry
                          .toOption
                          .map { _.value }
                      }
                  case EntryState.Removed =>
                    none[V].pure[F]
                }
            }
          }
      }

      def get1(key: K): F[Option[Either[F[V], V]]] = {
        entryMap
          .lookup(key)
          .flatMap { _.flatTraverse { _.optEither } }
      }

      def getOrUpdate(key: K)(value: => F[V]): F[V] = {
        getOrUpdate1(key) { value.map { a => (a, a, none[Release]) } }.flatMap {
          case Right(Right(a)) => a.pure[F]
          case Right(Left(a)) => a
          case Left(a) => a.pure[F]
        }
      }

      /**
       * Returns the value of the key, computing it if the key is not in the cache yet.
       *
       * The result tells a cache hit from a miss without waiting for anything:
       *   - `Left` if this call did compute the value;
       *   - `Right(Right)` if the value came from the cache, already computed;
       *   - `Right(Left)` if the value came from the cache and is still being computed by another
       *     fiber.
       *
       * The flow is: look the key up and return what is there, or, if there is nothing, install a
       * `Loading` entry and compute the value. Installing the entry is masked and done with a
       * single per-key CAS, so of the fibers racing to install one exactly one wins and the losers
       * simply await its `Deferred`. A `Removed` entry is a stale reference, and means the lookup
       * has to be repeated.
       */
      def getOrUpdate1[A](key: K)(value: => F[(A, V, Option[Release])]): F[Either[A, Either[F[V], V]]] = {

        /* Runs the value computation for the `Loading` entry this fiber installed, and publishes
         * its result.
         *
         * The computation is the only cancelable part of `getOrUpdate1`, hence it runs under
         * `poll`, and `cleanupOnCancel` has to undo the installed entry: unlink the key, complete
         * the `deferred` with `CancelledError` to unblock the waiters, and release the value if
         * the computation completed before the cancellation was observed.
         *
         * The computation is raced against `deferred` to also handle being overtaken by a `put` of
         * the same key: whoever completes the `deferred` first defines the value of the entry, and
         * the loser releases the value it produced.
         */
        def load(
          poll: Poll[F],
          entryRef: EntryRef[F, V],
          deferred: DeferredThrow[F, Entry[F, V]],
        ): F[Either[A, Either[F[V], V]]] = {
          Ref[F].of(none[Entry[F, V]]).flatMap { computed =>
            val cleanupOnCancel =
              entryRef
                .modify {
                  case state: EntryState.Loading[F, V] if state.deferred == deferred =>
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
                  case false =>
                    ().pure[F]
                }
                // Completed regardless of whether the entry was still ours: the waiters hold this
                // very `deferred`, and if the entry was taken away without completing it, as
                // `remove` does, we are the only one left to unblock them. A `deferred` already
                // completed by `put` ignores this.
                .productR { deferred.complete(CancelledError.asLeft).void }
                .productR {
                  computed
                    .get
                    .flatMap { _.foldMapM { _.release1 } }
                }

            poll {
              F.uncancelable {
                _ {
                  value.map { case (a, value, release) =>
                    val entry = entryOf(value, release)
                    (a, entry)
                  }
                }
                  .flatTap { case (_, entry) => computed.set(entry.some) }
              }
                .attempt
                .race1 { deferred.get }
            }
              .onCancel { cleanupOnCancel }
              .flatMap {
                // `value` got computed, and deferred was not (yet) completed by any other fiber in `put`
                case Left(Right((a, entry))) =>
                  deferred
                    .complete(entry.asRight)
                    .flatMap {
                      // Successfully completed our deferred,
                      // now trying to place the new value in the entry.
                      case true =>

                        def releaseAndReturnValue(state: EntryState.Value[F, V]): F[Either[A, Either[F[V], V]]] =
                          entry
                            .release1
                            .start
                            .as {
                              state
                                .entry
                                .value
                                .asRight[F[V]]
                                .asRight[A]
                            }

                        def releaseAndReturnLoading(state: EntryState.Loading[F, V]): F[Either[A, Either[F[V], V]]] =
                          entry
                            .release1
                            .start
                            .as {
                              state
                                .deferred
                                .getOrError
                                .map(_.value)
                                .asLeft[V]
                                .asRight[A]
                            }

                        // Try putting computed value in the map, if there is no entry with our key.
                        // If the map already contains an entry with our key,
                        // return its value (or value computation).
                        def tryPutNewValue: F[Either[A, Either[F[V], V]]] =
                          Ref[F]
                            .of[EntryState[F, V]](EntryState.Value(entry))
                            .flatMap { newEntryRef =>
                              ().tailRecM { _ =>
                                entryMap
                                  .ref(key)
                                  .modify {
                                    case None => (newEntryRef.some, none[EntryRef[F, V]])
                                    case some => (some, some)
                                  }
                                  .flatMap {
                                    case None =>
                                      a
                                        .asLeft[Either[F[V], V]]
                                        .asRight[Unit]
                                        .pure[F]
                                    case Some(existingRef) =>
                                      existingRef
                                        .get
                                        .flatMap {
                                          case state: EntryState.Value[F, V] =>
                                            releaseAndReturnValue(state).map(_.asRight[Unit])

                                          case state: EntryState.Loading[F, V] =>
                                            releaseAndReturnLoading(state).map(_.asRight[Unit])

                                          // `Removed` means that this entry won't be present in the map
                                          // next time we look the key up (see `remove` flow),
                                          // so we just retry.
                                          case EntryState.Removed =>
                                            ()
                                              .asLeft[Either[A, Either[F[V], V]]]
                                              .pure[F]
                                        }
                                  }
                              }
                            }

                        entryRef
                          .access
                          .flatMap {
                            // Entry is still in loading state, containing the same deferred we just completed.
                            // Now we can try to put the computed value in the same entryRef.
                            case (state: EntryState.Loading[F, V], set) if state.deferred == deferred =>
                              set(EntryState.Value(entry))
                                .flatMap {
                                  // Happy path: successfully placed our computed value
                                  case true =>
                                    a
                                      .asLeft[Either[F[V], V]]
                                      .pure[F]

                                  // Failed to set our value: while we were loading, `put`, `modify`,
                                  // `remove` or `clear` got to the same entry, so it was either:
                                  // - Updated: in that case we release our computed value, and return
                                  //   the value (or its computation), giving it the priority
                                  // - Removed: in that case we try to put our value back in the map
                                  case false =>
                                    entryRef
                                      .get
                                      .flatMap {
                                        case state: EntryState.Value[F, V] =>
                                          releaseAndReturnValue(state)

                                        case state: EntryState.Loading[F, V] =>
                                          releaseAndReturnLoading(state)

                                        case EntryState.Removed =>
                                          tryPutNewValue
                                      }
                                }

                            case (state: EntryState.Value[F, V], _) =>
                              releaseAndReturnValue(state)

                            case (state: EntryState.Loading[F, V], _) =>
                              releaseAndReturnLoading(state)

                            case (EntryState.Removed, _) =>
                              tryPutNewValue
                          }

                      // Deferred got completed by another fiber, so we return what they put there,
                      // and release the value we just computed.
                      case false =>
                        entry
                          .release1
                          .start
                          .productR(
                            deferred
                              .getOrError
                              .map { entry =>
                                entry
                                  .value
                                  .asRight[F[V]]
                                  .asRight[A]
                              },
                          )
                    }

                // `value` computation completed with error,
                // and deferred was not completed in another fiber in `put`.
                case Left(Left(error)) =>
                  deferred
                    .complete(error.asLeft)
                    .flatMap {
                      // Successfully completed our deferred with error,
                      // now trying to remove the entry from the map, if it is still there.
                      case true =>
                        entryMap
                          .ref(key)
                          .modify {
                            // The entry we added to the map is still there and unmodified,
                            // so we can safely remove it and propagate the error
                            case Some(`entryRef`) => (none[EntryRef[F, V]], none[EntryRef[F, V]])
                            case other => (other, other)
                          }
                          .flatMap {
                            // Key was removed (or removed and replaced by us) while we were loading,
                            // so we are just propagating the error
                            case None =>
                              error.raiseError[F, Either[F[V], V]]
                            // Another fiber replaced the entry we added to the map,
                            // so we return their value (computed or ongoing),
                            // or propagate our error if their entry got removed.
                            case Some(otherRef) =>
                              otherRef
                                .optEither
                                .flatMap(_.liftTo[F](error))
                          }

                      // Someone else completed the deferred before us, so they must've take care of
                      // updating the entry, and we return their result.
                      case false =>
                        deferred
                          .getOrError
                          .map { _.value }
                          .asLeft[V]
                          .pure[F]
                    }
                    .map { _.asRight[A] }

                // Deferred was completed by `put` in another fiber before `value` computation completed.
                // We return their value, and schedule release of our value that is still being computed.
                case Right((fiber, entry)) =>
                  fiber
                    .joinWithNever
                    .flatMap {
                      case Right((_, entry)) => entry.release1
                      case _ => ().pure[F]
                    }
                    .start
                    .productR {
                      entry
                        .liftTo[F]
                        .map { entry =>
                          entry
                            .value
                            .asRight[F[V]]
                            .asRight[A]
                        }
                    }
              }
          }
        }

        ().tailRecM { _ =>
          entryMap
            .lookup(key)
            .flatMap {
              case Some(entryRef) =>
                entryRef
                  .optEither
                  .map {
                    case Some(either) =>
                      either
                        .asRight[A]
                        .asRight[Unit]
                    // Entry got removed (see `remove` flow), so we retry expecting to get something else with our key.
                    case None =>
                      ().asLeft[Either[A, Either[F[V], V]]]
                  }
              case None =>
                F.uncancelable { poll =>
                  for {
                    deferred <- Deferred[F, Either[Throwable, Entry[F, V]]]
                    entryRef <- Ref[F].of[EntryState[F, V]](EntryState.Loading(deferred))
                    existing <- entryMap
                      .ref(key)
                      .modify {
                        case None => (entryRef.some, none[EntryRef[F, V]])
                        case some => (some, some)
                      }
                    result <- existing match {
                      case Some(existingRef) =>
                        existingRef
                          .optEither
                          .map {
                            case Some(either) =>
                              either
                                .asRight[A]
                                .asRight[Unit]
                            case None =>
                              ().asLeft[Either[A, Either[F[V], V]]]
                          }
                      case None =>
                        load(poll, entryRef, deferred).map { _.asRight[Unit] }
                    }
                  } yield result
                }
            }
        }
      }

      /**
       * Stores the value under the key, returning the replaced value, if any.
       *
       * The outer effect performs the replacement, the inner one awaits the release of the replaced
       * value, so that the caller can decide whether to wait for it.
       *
       * A `Loading` entry is not waited for: its `deferred` is completed with the new value, which
       * both unblocks the waiters immediately and tells the loading fiber that it lost the race and
       * has to release the value it computes.
       */
      def put(key: K, value: V, release: Option[Release]): F[F[Option[V]]] = {
        val entry = entryOf(value, release)

        // Our value did not make it into the map, so nothing was replaced and we own its release,
        // which we start and forget, as no caller is waiting for it.
        def releaseAndExit: F[Either[Unit, F[Option[V]]]] = {
          entry
            .release
            .traverse { _.start }
            .as {
              none[V]
                .pure[F]
                .asRight[Unit]
            }
        }

        ().tailRecM { _ =>
          entryMap
            .lookup(key)
            .flatMap {
              case None =>
                // No entry present in the map, so we add a new one
                Ref[F]
                  .of[EntryState[F, V]](EntryState.Value(entry))
                  .flatMap { entryRef =>
                    entryMap
                      .ref(key)
                      .modify {
                        case None => (entryRef.some, true)
                        case some => (some, false)
                      }
                      .map {
                        case true =>
                          none[V]
                            .pure[F]
                            .asRight[Unit]
                        case false =>
                          ().asLeft[F[Option[V]]]
                      }
                  }
              case Some(entryRef) =>
                entryRef
                  .access
                  .flatMap {
                    // A computed value is already present in the map, so we are replacing it with our value.
                    case (state: EntryState.Value[F, V], set) =>
                      set(EntryState.Value(entry))
                        .flatMap {
                          // Successfully replaced the entryRef with our value,
                          // now we are responsible for releasing the old value.
                          case true =>
                            state
                              .entry
                              .release
                              .traverse { _.start }
                              .map { fiber =>
                                fiber
                                  .foldMapM { _.joinWithNever }
                                  .as { state.entry.value.some }
                                  .asRight[Unit]
                              }

                          // Failed to set the entryRef to our value
                          // so we just release our value and exit.
                          case false =>
                            releaseAndExit
                        }

                    // The value is still loading, so we first try to complete the deferred with it,
                    // and then replace it with our value.
                    case (state: EntryState.Loading[F, V], set) =>
                      state
                        .deferred
                        .complete(entry.asRight)
                        .flatMap {
                          // We successfully completed the deferred, now trying to set the value.
                          case true =>
                            set(EntryState.Value(entry)).flatMap {
                              // We successfully replaced the entry with our value, so we are done.
                              case true =>
                                none[V]
                                  .pure[F]
                                  .asRight[Unit]
                                  .pure[F]

                              // Another fiber placed their new value before us
                              // so we just release our value and exit.
                              case false =>
                                releaseAndExit
                            }

                          // Someone just completed the deferred we saw
                          // so we just release our value and exit.
                          case false =>
                            releaseAndExit
                        }

                    // The key was just removed from the map, so just release the value and exit.
                    case (EntryState.Removed, _) =>
                      releaseAndExit
                  }
                  .uncancelable
            }
        }
      }

      /**
       * Applies the decision of `f` to the current value of the key, atomically.
       *
       * `f` is called with the value of the key, or `None` if there is none, and may be called more
       * than once, because a lost CAS means the decision was made on a stale value and has to be
       * taken again. A `Loading` entry is presented to `f` as `None`, as there is no value to
       * decide upon yet, and is only overwritten if `f` decides to put one.
       */
      override def modify[A](key: K)(f: Option[V] => (A, Directive[F, V])): F[(A, Option[F[Unit]])] = {
        ().tailRecM { _ =>
          entryMap
            .lookup(key)
            .flatMap {
              case None =>
                f(None) match {
                  // No entry present in the map, and we want to add a new one
                  case (a, put: Directive.Put[F, V]) =>
                    Ref[F]
                      .of[EntryState[F, V]](EntryState.Value(entryOf(put.value, put.release)))
                      .flatMap { entryRef =>
                        entryMap
                          .ref(key)
                          .modify {
                            case None => (entryRef.some, true)
                            case some => (some, false)
                          }
                          .map {
                            case true =>
                              (a, none[F[Unit]])
                                .asRight[Unit]
                            // Failed adding new entry to the map, retrying accessing the map
                            case false =>
                              ().asLeft[(A, Option[F[Unit]])]
                          }
                      }
                  // No entry present in the map, and we don't want to have any, so exiting
                  case (a, Directive.Ignore | Directive.Remove) =>
                    (a, none[F[Unit]])
                      .asRight[Unit]
                      .pure[F]
                }
              case Some(entryRef) =>
                ().tailRecM { _ =>
                  entryRef
                    .access
                    .flatMap {
                      // A value is already present in the map
                      case (state: EntryState.Value[F, V], setRef) =>
                        f(state.entry.value.some) match {
                          case (a, put: Directive.Put[F, V]) =>
                            setRef(EntryState.Value(entryOf(put.value, put.release)))
                              .flatMap {
                                // Successfully replaced the entryRef with our value,
                                // now we are responsible for releasing the old value.
                                case true =>
                                  state
                                    .entry
                                    .release
                                    .traverse { _.start }
                                    .map { release =>
                                      (a, release.map(_.joinWithNever))
                                        .asRight[Unit]
                                        .asRight[Unit]
                                    }
                                // Failed updating entryRef, retrying
                                case false =>
                                  ()
                                    .asLeft[Either[Unit, (A, Option[F[Unit]])]]
                                    .pure[F]
                              }
                          // Keeping the value intact and exiting
                          case (a, Directive.Ignore) =>
                            (a, none[F[Unit]])
                              .asRight[Unit]
                              .asRight[Unit]
                              .pure[F]
                          // Removing the value
                          case (a, Directive.Remove) =>
                            setRef(EntryState.Removed)
                              .flatMap {
                                // Successfully set the entryRef to `Removed` state, now removing it from the map.
                                // Only removing the key if it still contains this entry, otherwise noop.
                                case true =>
                                  entryMap
                                    .ref(key)
                                    .update {
                                      case Some(`entryRef`) => none
                                      case other => other
                                    }
                                    .flatMap { _ =>
                                      // Releasing the value regardless of the map update result.
                                      state
                                        .entry
                                        .release
                                        .traverse { _.start }
                                        .map { release =>
                                          (a, release.map(_.joinWithNever))
                                            .asRight[Unit]
                                            .asRight[Unit]
                                        }
                                    }
                                // Failed updating entryRef, retrying
                                case false =>
                                  ()
                                    .asLeft[Either[Unit, (A, Option[F[Unit]])]]
                                    .pure[F]
                              }
                        }

                      // Entry in the map is still loading
                      case (state: EntryState.Loading[F, V], setRef) =>
                        f(None) match {
                          // Trying to replace it with our value
                          case (a, put: Directive.Put[F, V]) =>
                            val entry = entryOf(put.value, put.release)
                            state
                              .deferred
                              .complete(entry.asRight)
                              .flatMap {
                                // We successfully completed the deferred, now trying to set the value.
                                case true =>
                                  setRef(EntryState.Value(entry)).flatMap {
                                    // We successfully replaced the entry with our value, so we are done.
                                    case true =>
                                      (a, none[F[Unit]])
                                        .asRight[Unit]
                                        .asRight[Unit]
                                        .pure[F]
                                    // The entry was removed (only Removed is possible here) before our
                                    // value made it in. Completing the deferred made us the owner of the
                                    // release, and published the decision to the waiters, so instead of
                                    // deciding again we release the value and exit, the same way `put`
                                    // does when its value loses this very race.
                                    case false =>
                                      entry
                                        .release
                                        .traverse { _.start }
                                        .as {
                                          (a, none[F[Unit]])
                                            .asRight[Unit]
                                            .asRight[Unit]
                                        }
                                  }
                                // Failed to complete the deferred, meaning someone else completed it, and will
                                // now set the new value in the entryRef. Retrying the lookup.
                                case false =>
                                  ()
                                    .asLeft[Either[Unit, (A, Option[F[Unit]])]]
                                    .pure[F]
                              }
                          // Noop decision, exiting
                          case (a, Directive.Ignore | Directive.Remove) =>
                            (a, none[F[Unit]])
                              .asRight[Unit]
                              .asRight[Unit]
                              .pure[F]
                        }

                      // Entry was just removed, it soon will be gone from the map.
                      case (EntryState.Removed, _) =>
                        f(None) match {
                          // We want to place the new value;
                          // Retrying the map lookup, expecting a different result for our key.
                          case (_, _: Directive.Put[F, V]) =>
                            ()
                              .asLeft[(A, Option[F[Unit]])]
                              .asRight[Unit]
                              .pure[F]
                          // Noop decision, exiting
                          case (a, Directive.Ignore | Directive.Remove) =>
                            (a, none[F[Unit]])
                              .asRight[Unit]
                              .asRight[Unit]
                              .pure[F]
                        }
                    }
                    .uncancelable
                }
            }
        }
      }

      def contains(key: K): F[Boolean] = entryMap.contains(key)

      def size: F[Int] = entryMap.size

      def keys: F[Set[K]] = entryMap.keys

      def values: F[Map[K, F[V]]] = {
        entryMap
          .entries
          .flatMap { entries =>
            entries
              .foldLeft {
                List
                  .empty[(K, F[V])]
                  .pure[F]
              } { case (values, (key, entryRef)) =>
                values.flatMap { values =>
                  entryRef
                    .value
                    .map {
                      case Some(value) => (key, value) :: values
                      case None => values
                    }
                }
              }
          }
          .map { _.toMap }
      }

      def values1: F[Map[K, Either[F[V], V]]] = {
        entryMap
          .entries
          .flatMap { entries =>
            entries
              .foldLeft {
                List
                  .empty[(K, Either[F[V], V])]
                  .pure[F]
              } { case (values, (key, entryRef)) =>
                values.flatMap { values =>
                  entryRef
                    .optEither
                    .map {
                      case Some(value) => (key, value) :: values
                      case None => values
                    }
                }
              }
          }
          .map { _.toMap }
      }

      /**
       * Removes the key from the cache, returning the removed value, if any.
       *
       * Unlinking the key and marking the entry `Removed` happen in that order and uncancelably:
       * the mark is what makes this fiber the one responsible for the release, and what tells the
       * fibers holding this `EntryRef` that they are looking at a stale reference.
       *
       * A `Loading` entry has no value to return, and removing it does not cancel the load: the
       * loading fiber finds the `Removed` mark, sees that the key is now free, and stores its value
       * under it, so a load that outlives the `remove` puts the key back into the cache.
       */
      def remove(key: K): F[F[Option[V]]] = {
        entryMap
          .ref(key)
          .getAndSet(none)
          .flatMap {
            case Some(entryRef) =>
              // We just removed the entry from the map, now we need to release it.
              // Replacing the value of the ref with `Removed` means that we are getting responsible for the release.
              entryRef
                .getAndSet(EntryState.Removed)
                .flatMap {
                  // We removed a loaded value, so we are responsible for releasing it.
                  case state: EntryState.Value[F, V] =>
                    state
                      .entry
                      .release1
                      .as { state.entry.value.some }
                      .start
                      .map { fiber =>
                        fiber.joinWithNever
                      }

                  // We removed a loading value, and the fiber that will complete it will also
                  // release that value, so there is nothing for us to return.
                  case _: EntryState.Loading[F, V] =>
                    none[V]
                      .pure[F]
                      .pure[F]

                  // We removed an entry that was already being removed by another fiber, so we are done.
                  case EntryState.Removed =>
                    none[V]
                      .pure[F]
                      .pure[F]
                }
            case None =>
              none[V]
                .pure[F]
                .pure[F]
          }
          .uncancelable
      }

      /**
       * Removes all the entries, returning an effect awaiting the release of all their values.
       *
       * The keys are unlinked one by one, as there is no atomic bulk operation on a per-key `Ref`,
       * so entries added concurrently may survive the clearing. As this also runs on the release of
       * the cache resource, an entry added while a large cache is being cleared can outlive the
       * cache itself, with its value never released.
       *
       * Values of entries that are still loading are awaited before being released, which is why a
       * load that never completes would make this, and the release of the cache resource, hang.
       */
      def clear: F[F[Unit]] = {
        entryMap
          .keys
          .flatMap { keys =>
            keys
              .toList
              .traverse { key => entryMap.ref(key).getAndSet(none) }
              .map { _.flatten }
          }
          .flatMap { entryRefs =>
            entryRefs
              .parFoldMap1 { entryRef =>
                entryRef
                  .getOption
                  .flatMap { _.foldMapM { _.release1 } }
                  .uncancelable
              }
              .start
          }
          .uncancelable
          .map { _.joinWithNever }
      }

      def foldMap[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]): F[A] = {
        entryMap
          .entries
          .flatMap { entries =>
            val zero = CommutativeMonoid[A]
              .empty
              .pure[F]
            entries.foldLeft(zero) { case (a, (key, entryRef)) =>
              for {
                a <- a
                v <- entryRef.optEither
                b <- v.fold(CommutativeMonoid[A].empty.pure[F])(v => f(key, v))
              } yield {
                CommutativeMonoid[A].combine(a, b)
              }
            }
          }
      }

      def foldMapPar[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]): F[A] = {
        entryMap
          .entries
          .flatMap { entries =>
            Parallel[F].sequential {
              val zero = Parallel[F]
                .applicative
                .pure(CommutativeMonoid[A].empty)
              entries
                .foldLeft(zero) { case (a, (key, entryRef)) =>
                  val b = Parallel[F].parallel {
                    for {
                      v <- entryRef.optEither
                      b <- v.fold(CommutativeMonoid[A].empty.pure[F])(v => f(key, v))
                    } yield b
                  }
                  Parallel[F]
                    .applicative
                    .map2(a, b)(CommutativeMonoid[A].combine)
                }
            }
          }
      }
    }
  }

  /**
   * Cached value together with the effect releasing it, if it needs releasing.
   */
  final case class Entry[+F[_], +A](value: A, release: Option[F[Unit]])

  object Entry {
    implicit class EntryOps[F[_], A](val self: Entry[F, A]) extends AnyVal {
      def release1(
        implicit
        F: Monad[F],
      ): F[Unit] = self.release.foldA
    }
  }

  /**
   * State of a cache entry.
   *
   * The possible transitions are `Loading -> Value`, `Loading -> Removed`, `Value -> Value` and
   * `Value -> Removed`, with `Removed` being terminal, so that a stale reference stays recognizable
   * as such.
   */
  sealed trait EntryState[+F[_], +A]
  object EntryState {

    /**
     * The value is being computed, and `deferred` will hold it, or the reason it will never be
     * available: [[CancelledError]] if the load got cancelled, [[ExpiredError]] if it was evicted
     * for taking too long, or the error the computation failed with.
     *
     * The `deferred` doubles as the identity of the load: a fiber may only act on the entry as long
     * as it still holds the very same `deferred` it installed, which is what keeps a fiber from
     * interfering with a load started after its own one ended.
     */
    final case class Loading[F[_], A](deferred: Deferred[F, Either[Throwable, Entry[F, A]]]) extends EntryState[F, A]

    /**
     * The value is computed and available.
     */
    final case class Value[F[_], A](entry: Entry[F, A]) extends EntryState[F, A]

    /**
     * The entry is gone, and this reference to it is stale: the key it used to be mapped to is
     * either unlinked already, or is about to be, and has to be looked up anew.
     */
    case object Removed extends EntryState[Nothing, Nothing]
  }

  type DeferredThrow[F[_], A] = Deferred[F, Either[Throwable, A]]

  type EntryRef[F[_], A] = Ref[F, EntryState[F, A]]

  implicit class DeferredThrowOps[F[_], A](val self: DeferredThrow[F, A]) extends AnyVal {
    def getOrError(
      implicit
      F: MonadThrow[F],
    ): F[A] = {
      self
        .get
        .flatMap {
          case Right(a) => a.pure[F]
          case Left(a) => a.raiseError[F, A]
        }
    }

    def getOption(
      implicit
      F: Functor[F],
    ): F[Option[A]] = {
      self
        .get
        .map { _.toOption }
    }
  }

  implicit class EntryStateOps[F[_], A](val self: EntryState[F, A]) extends AnyVal {

    /**
     * Value of the entry, awaiting it if it is still loading, and `None` if it will never arrive.
     */
    def getOption(
      implicit
      F: Applicative[F],
    ): F[Option[Entry[F, A]]] = {
      self match {
        case EntryState.Loading(deferred: Deferred[F, Either[Throwable, Entry[F, A]]]) => deferred.getOption
        case EntryState.Value(entry) => entry.some.pure[F]
        case EntryState.Removed => none[Entry[F, A]].pure[F]
      }
    }

    /**
     * The entry as the cache API sees it: `None` for an entry that is gone, `Left` for a value that
     * is still being computed, and `Right` for a value that is already there.
     */
    def optEither(
      implicit
      F: MonadThrow[F],
    ): Option[Either[F[A], A]] =
      self match {
        case EntryState.Value(entry) =>
          entry
            .value
            .asRight[F[A]]
            .some
        case EntryState.Loading(deferred: Deferred[F, Either[Throwable, Entry[F, A]]]) =>
          deferred
            .getOrError
            .map(_.value)
            .asLeft[A]
            .some
        case EntryState.Removed =>
          none[Either[F[A], A]]
      }

  }

  implicit class EntryRefOps[F[_], A](val self: EntryRef[F, A]) extends AnyVal {

    def getOption(
      implicit
      F: Monad[F],
    ): F[Option[Entry[F, A]]] = {
      self
        .get
        .flatMap(_.getOption)
    }

    def optEither(
      implicit
      F: MonadThrow[F],
    ): F[Option[Either[F[A], A]]] = {
      self
        .get
        .map(_.optEither)
    }

    def value(
      implicit
      F: MonadThrow[F],
    ): F[Option[F[A]]] = {
      self
        .get
        .map {
          case EntryState.Value(entry) =>
            entry
              .value
              .pure[F]
              .some
          case EntryState.Loading(deferred: Deferred[F, Either[Throwable, Entry[F, A]]]) =>
            deferred
              .getOrError
              .map { _.value }
              .some
          case EntryState.Removed =>
            none[F[A]]
        }
    }

    /**
     * Updates the value of the entry, if there is one, retrying on a lost CAS, and doing nothing at
     * all if the entry is still loading or is gone.
     */
    def update1(
      f: A => A,
    )(implicit
      F: Monad[F],
    ): F[Unit] = {
      0.tailRecM { counter =>
        self
          .access
          .flatMap {
            case (EntryState.Value(entry), set) =>
              val entry1 = entry.copy(value = f(entry.value))
              set(EntryState.Value(entry1)).map {
                case true => ().asRight[Int]
                case false => (counter + 1).asLeft[Unit]
              }
            case (_: EntryState.Loading[F, A], _) =>
              ()
                .asRight[Int]
                .pure[F]
            case (EntryState.Removed, _) =>
              ()
                .asRight[Int]
                .pure[F]
          }
      }
    }
  }

  implicit class Ops[F[_], A, E](val fa: F[A]) extends AnyVal {

    /**
     * Races `fa` against `fb`, returning `Left` if `fa` won, and `Right` with the still running
     * `fa` if `fb` did.
     *
     * Unlike `race`, a losing `fa` is not cancelled, but handed over to the caller instead, because
     * `fa` computes a value that will have to be released once it is there. A cancelled `fa`
     * cancels the race, while a cancelled `fb` leaves the race waiting for `fa`.
     */
    def race1[B](
      fb: F[B],
    )(implicit
      F: GenConcurrent[F, E],
    ): F[Either[A, (Fiber[F, E, A], B)]] = {
      import F.*
      uncancelable { poll =>
        poll(racePair(fa, fb)).flatMap {
          case Left((a, fiber)) =>
            a match {
              case Outcome.Succeeded(a) =>
                fiber
                  .cancel
                  .productR { a }
                  .map { _.asLeft }
              case Outcome.Errored(a) =>
                fiber
                  .cancel
                  .productR { raiseError(a) }
              case Outcome.Canceled() =>
                poll(canceled) *> never
            }
          case Right((fiber, b)) =>
            b match {
              case Outcome.Succeeded(b) => b.map { b => (fiber, b).asRight[A] }
              case Outcome.Errored(eb) => raiseError(eb)
              case Outcome.Canceled() =>
                poll(fiber.join)
                  .onCancel(fiber.cancel)
                  .flatMap {
                    case Outcome.Succeeded(a) => a.map { _.asLeft[(Fiber[F, E, A], B)] }
                    case Outcome.Errored(a) => raiseError(a)
                    case Outcome.Canceled() => poll(canceled) *> never
                  }
            }
        }
      }
    }
  }
}
