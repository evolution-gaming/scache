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

private[scache] object LoadingCache {

  def of[F[_]: Async, K, V]: Resource[F, Cache[F, K, V]] = {
    for {
      entryMap <- EntryMap.of[F, K, V].toResource
      cache <- of(entryMap)
    } yield cache
  }

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
   */
  trait EntryMap[F[_], K, V] {

    def ref(key: K): Ref[F, Option[EntryRef[F, V]]]

    def lookup(key: K): F[Option[EntryRef[F, V]]]

    def keys: F[Set[K]]

    def entries: F[List[(K, EntryRef[F, V])]]

    def size: F[Int]

    def contains(key: K): F[Boolean]
  }

  object EntryMap {

    def of[F[_]: Sync, K, V]: F[EntryMap[F, K, V]] = {
      Sync[F]
        .delay { new ConcurrentHashMap[K, EntryRef[F, V]]() }
        .map { chm => apply(chm) }
    }

    def apply[F[_]: Sync, K, V](chm: ConcurrentHashMap[K, EntryRef[F, V]]): EntryMap[F, K, V] = {
      val mapRef = MapRef.fromConcurrentHashMap[F, K, EntryRef[F, V]](chm)
      new EntryMap[F, K, V] {

        def ref(key: K): Ref[F, Option[EntryRef[F, V]]] = mapRef(key)

        def lookup(key: K): F[Option[EntryRef[F, V]]] = Sync[F].delay { Option(chm.get(key)) }

        def keys: F[Set[K]] = {
          Sync[F].delay { chm.keySet().asScala.toSet }
        }

        def entries: F[List[(K, EntryRef[F, V])]] = {
          Sync[F].delay {
            chm
              .entrySet()
              .iterator()
              .asScala
              .map { entry => (entry.getKey, entry.getValue) }
              .toList
          }
        }

        def size: F[Int] = Sync[F].delay { chm.mappingCount().toInt }

        def contains(key: K): F[Boolean] = Sync[F].delay { chm.containsKey(key) }
      }
    }
  }

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

      def getOrUpdate1[A](key: K)(value: => F[(A, V, Option[Release])]): F[Either[A, Either[F[V], V]]] = {

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
                      .productR { deferred.complete(CancelledError.asLeft).void }
                  case false =>
                    ().pure[F]
                }
                .productR {
                  computed
                    .get
                    .flatMap { _.foldMapM { _.release1 } }
                }

            poll {
              F.uncancelable { poll1 =>
                poll1 {
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
                            .flatMap { newRef =>
                              ().tailRecM { _ =>
                                entryMap
                                  .ref(key)
                                  .modify {
                                    case None => (newRef.some, none[EntryRef[F, V]])
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
                                  // Failed to set our value, meaning the entry was either:
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

      def put(key: K, value: V, release: Option[Release]): F[F[Option[V]]] = {
        val entry = entryOf(value, release)
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
                            entry
                              .release
                              .traverse { _.start } // Start releasing and forget
                              .as {
                                none[V]
                                  .pure[F]
                                  .asRight[Unit]
                              }
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
                                entry
                                  .release
                                  .traverse { _.start } // Start releasing and forget
                                  .as {
                                    none[V]
                                      .pure[F]
                                      .asRight[Unit]
                                  }
                            }
                          // Someone just completed the deferred we saw
                          // so we just release our value and exit.
                          case false =>
                            entry
                              .release
                              .traverse { _.start } // Start releasing and forget
                              .as {
                                none[V]
                                  .pure[F]
                                  .asRight[Unit]
                              }
                        }

                    // The key was just removed from the map, so just release the value and exit.
                    case (EntryState.Removed, _) =>
                      entry
                        .release
                        .traverse { _.start } // Start releasing and forget
                        .as {
                          none[V]
                            .pure[F]
                            .asRight[Unit]
                        }
                  }
                  .uncancelable
            }
        }
      }

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
                                  setRef(EntryState.Value(entry)).map {
                                    // We successfully replaced the entry with our value, so we are done.
                                    case true =>
                                      (a, none[F[Unit]])
                                        .asRight[Unit]
                                        .asRight[Unit]
                                    // Another fiber placed their new value (only Removed should be possible)
                                    // before us so we retry accessing the entry.
                                    case false =>
                                      ().asLeft[Either[Unit, (A, Option[F[Unit]])]]
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

  final case class Entry[+F[_], +A](value: A, release: Option[F[Unit]])

  object Entry {
    implicit class EntryOps[F[_], A](val self: Entry[F, A]) extends AnyVal {
      def release1(
        implicit
        F: Monad[F],
      ): F[Unit] = self.release.foldA
    }
  }

  sealed trait EntryState[+F[_], +A]
  object EntryState {
    final case class Loading[F[_], A](deferred: Deferred[F, Either[Throwable, Entry[F, A]]]) extends EntryState[F, A]
    final case class Value[F[_], A](entry: Entry[F, A]) extends EntryState[F, A]
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
