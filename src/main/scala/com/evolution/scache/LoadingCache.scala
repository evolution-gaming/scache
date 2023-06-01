package com.evolution.scache

import cats.{Applicative, Functor, Monad, MonadThrow, Parallel}
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.syntax.all._
import cats.effect.{Concurrent, Fiber, Resource}
import cats.kernel.CommutativeMonoid
import cats.syntax.all._
import com.evolution.scache.Cache.Directive
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.ParallelHelper._

private[scache] object LoadingCache {

  def of[F[_] : Concurrent: Parallel, K, V](
    map: EntryRefs[F, K, V],
  ): Resource[F, Cache[F, K, V]] = {
    for {
      ref   <- Ref[F].of(map).toResource
      cache <- of(ref)
    } yield cache
  }


  def of[F[_] : Concurrent: Parallel, K, V](
    ref: Ref[F, EntryRefs[F, K, V]],
  ): Resource[F, Cache[F, K, V]] = {
    Resource.make {
      apply(ref).pure[F]
    } { cache =>
      cache.clear.flatten
    }
  }

  def apply[F[_]: Concurrent: Parallel, K, V](
    ref: Ref[F, EntryRefs[F, K, V]]
  ): Cache[F, K, V] = {

    val ignore = (_: Throwable) => ()

    def entryOf(value: V, release: Option[F[Unit]]) = {
      Entry(
        value = value,
        release = release.map { _.handleError(ignore) })
    }

    abstract class LoadingCache extends Cache.Abstract1[F, K, V]

    new LoadingCache {

      def get(key: K) = {
        ref
          .get
          .flatMap { entryRefs =>
            entryRefs
              .get(key)
              .fold {
                none[V].pure[F]
              } { entry =>
                entry
                  .get
                  .flatMap {
                    case state: EntryState.Value[F, V]   =>
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

      def get1(key: K) = {
        ref
          .get
          .flatMap { entryRefs =>
            entryRefs
              .get(key)
              .flatTraverse { _.optEither }
          }
      }

      def getOrUpdate(key: K)(value: => F[V]) = {
        getOrUpdate1(key) { value.map { a => (a, a, none[Release]) } }.flatMap {
          case Right(Right(a)) => a.pure[F]
          case Right(Left(a))  => a
          case Left(a)         => a.pure[F]
        }
      }

      def getOrUpdate1[A](key: K)(value: => F[(A, V, Option[Release])]): F[Either[A, Either[F[V], V]]] = {
        0.tailRecM { counter =>
          ref
            .access
            .flatMap { case (entryRefs, set) =>
              entryRefs
                .get(key)
                .fold {
                  for {
                    deferred <- Deferred[F, Either[Throwable, Entry[F, V]]]
                    entryRef <- Ref[F].of[EntryState[F, V]](EntryState.Loading(deferred))
                    result   <- set(entryRefs.updated(key, entryRef))
                      .flatMap {
                        case true =>
                          value
                            .map { case (a, value, release) =>
                              val entry = entryOf(value, release)
                              (a, entry)
                            }
                            .attempt
                            .race1 { deferred.get }
                            .flatMap {
                              // `value` got computed, and deferred was not (yet) completed by any other fiber in `put`
                              case Left(Right((a, entry))) =>
                                deferred
                                  .complete1(entry.asRight)
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
                                        0.tailRecM { counter =>
                                          ref
                                            .access
                                            .flatMap { case (entryRefs, set) =>
                                              entryRefs
                                                .get(key)
                                                .fold {
                                                  // No entry present in the map, so we try to add a new one
                                                  Ref[F]
                                                    .of[EntryState[F, V]](EntryState.Value(entry))
                                                    .flatMap { entryRef =>
                                                      set(entryRefs.updated(key, entryRef)).map {
                                                        case true =>
                                                          a
                                                            .asLeft[Either[F[V], V]]
                                                            .asRight[Int]
                                                        case false =>
                                                          (counter + 1)
                                                            .asLeft[Either[A, Either[F[V], V]]]
                                                      }
                                                    }
                                                } { entryRef =>
                                                  entryRef
                                                    .get
                                                    .flatMap {
                                                      case state: EntryState.Value[F, V] =>
                                                        releaseAndReturnValue(state).map(_.asRight[Int])

                                                      case state: EntryState.Loading[F, V] =>
                                                        releaseAndReturnLoading(state).map(_.asRight[Int])

                                                      // `Removed` means that this entry won't be present in the map
                                                      // next time we look the key up (see `remove` flow),
                                                      // so we just retry.
                                                      case EntryState.Removed =>
                                                        (counter + 1)
                                                          .asLeft[Either[A, Either[F[V], V]]]
                                                          .pure[F]
                                                    }
                                                    .uncancelable
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
                                            }
                                        )
                                  }

                              // `value` computation completed with error,
                              // and deferred was not completed in another fiber in `put`.
                              case Left(Left(error)) =>
                                deferred
                                  .complete1(error.asLeft)
                                  .flatMap {
                                    // Successfully completed our deferred with error,
                                    // now trying to remove the entry from the map, if it is still there.
                                    case true =>
                                      0.tailRecM { counter1 =>
                                        ref
                                          .access
                                          .flatMap { case (entryRefs, set) =>
                                            entryRefs
                                              .get(key)
                                              .fold {
                                                // Key was removed while we were loading,
                                                // so we are just propagating the error
                                                error.raiseError[F, Either[Int, Either[F[V], V]]]
                                              } {
                                                // The entry we added to the map is still there and unmodified,
                                                // so we can safely remove it and propagate the error
                                                case `entryRef` =>
                                                  set(entryRefs - key).flatMap {
                                                    // Happy path: successfully removed our entry
                                                    case true  =>
                                                      error.raiseError[F, Either[Int, Either[F[V], V]]]
                                                    // Retrying (different keys could've been modified in the map)
                                                    case false =>
                                                      (counter1 + 1)
                                                        .asLeft[Either[F[V], V]]
                                                        .pure[F]
                                                  }
                                                // Another fiber replaced the `ref` we added to the map,
                                                // so we return their value (computed or ongoing),
                                                // or propagate our error if our entry got removed.
                                                case entryRef =>
                                                  entryRef
                                                    .optEither
                                                    .flatMap(_.liftTo[F](error))
                                                    .map(_.asRight[Int])
                                              }
                                          }
                                      }

                                    // Someone else completed the deferred before us, so they must've take care of
                                    // updating the `ref`, and we return their result.
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
                                  .join
                                  .flatMap {
                                    case Right((_, entry)) => entry.release1
                                    case _                 => ().pure[F]
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
                            .map { _.asRight[Int] }

                        case false =>
                          (counter + 1)
                            .asLeft[Either[A, Either[F[V], V]]]
                            .pure[F]
                      }
                      .uncancelable
                  } yield result
                } { entryRef =>
                  // Map already contained an entry under our key, so we return that value (or its ongoing computation)
                  entryRef
                    .optEither
                    .map {
                      case Some(either) =>
                        either
                          .asRight[A]
                          .asRight[Int]
                      // Entry got removed (see `remove` flow), so we retry expecting to get something else with our key.
                      case None =>
                        (counter + 1)
                          .asLeft[Either[A, Either[F[V], V]]]
                    }
                }
            }
        }
      }

      def put(key: K, value: V, release: Option[Release]): F[F[Option[V]]] = {
        val entry = entryOf(value, release)
        0.tailRecM { counter =>
          ref
            .access
            .flatMap { case (entryRefs, set) =>
              entryRefs
                .get(key)
                .fold {
                  // No entry present in the map, so we add a new one
                  Ref[F]
                    .of[EntryState[F, V]](EntryState.Value(entry))
                    .flatMap { entryRef =>
                      set(entryRefs.updated(key, entryRef)).map {
                        case true =>
                          none[V]
                            .pure[F]
                            .asRight[Int]
                        case false =>
                          (counter + 1)
                            .asLeft[F[Option[V]]]
                      }
                    }
                } { entryRef =>
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
                                    .foldMapM { _.join }
                                    .as { state.entry.value.some }
                                    .asRight[Int]
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
                                    .asRight[Int]
                                }
                          }

                      // The value is still loading, so we first try to complete the deferred with it,
                      // and then replace it with our value.
                      case (state: EntryState.Loading[F, V], set) =>
                        state
                          .deferred
                          .complete1(entry.asRight)
                          .flatMap {
                            // We successfully completed the deferred, now trying to set the value.
                            case true =>
                              set(EntryState.Value(entry)).flatMap {
                                // We successfully replaced the entry with our value, so we are done.
                                case true =>
                                  none[V]
                                    .pure[F]
                                    .asRight[Int]
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
                                        .asRight[Int]
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
                                    .asRight[Int]
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
                              .asRight[Int]
                          }
                    }
                    .uncancelable
                }
            }
        }
      }

      override def modify[A](key: K)(f: Option[V] => (A, Directive[F, V])): F[(A, Option[F[Unit]])] = {
        0.tailRecM { counter =>
          ref
            .access
            .flatMap { case (entryRefs, setMap) =>
              entryRefs
                .get(key)
                .fold {
                  f(None) match {
                    // No entry present in the map, and we want to add a new one
                    case (a, put: Directive.Put[F, V]) =>
                      Ref[F]
                        .of[EntryState[F, V]](EntryState.Value(entryOf(put.value, put.release)))
                        .flatMap { entryRef =>
                          setMap(entryRefs.updated(key, entryRef)).map {
                            case true =>
                              (a, none[F[Unit]])
                                .asRight[Int]
                            // Failed adding new entry to the map, retrying accessing the map
                            case false =>
                              (counter + 1)
                                .asLeft[(A, Option[F[Unit]])]
                          }
                        }
                    // No entry present in the map, and we don't want to have any, so exiting
                    case (a, Directive.Ignore | Directive.Remove) =>
                      (a, none[F[Unit]])
                        .asRight[Int]
                        .pure[F]
                  }
                } { entryRef =>
                  0.tailRecM { counter1 =>
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
                                        (a, release.map(_.join))
                                          .asRight[Int]
                                          .asRight[Int]
                                      }
                                  // Failed updating entryRef, retrying
                                  case false =>
                                    (counter1 + 1)
                                     .asLeft[Either[Int, (A, Option[F[Unit]])]]
                                     .pure[F]
                                }
                            // Keeping the value intact and exiting
                            case (a, Directive.Ignore) =>
                              (a, none[F[Unit]])
                                .asRight[Int]
                                .asRight[Int]
                                .pure[F]
                            // Removing the value
                            case (a, Directive.Remove) =>
                              setRef(EntryState.Removed)
                                .flatMap {
                                  // Successfully set the entryRef to `Removed` state, now removing it from the map.
                                  // Only removing the key if it still contains this entry, otherwise noop.
                                  case true =>
                                    ref
                                      .update { entryRefs =>
                                        entryRefs.get(key) match {
                                          case Some(`entryRef`) => entryRefs - key
                                          case _ => entryRefs
                                        }
                                      }
                                      .flatMap { _ =>
                                        // Releasing the value regardless of the map update result.
                                        state
                                          .entry
                                          .release
                                          .traverse { _.start }
                                          .map { release =>
                                            (a, release.map(_.join))
                                              .asRight[Int]
                                              .asRight[Int]
                                          }
                                      }
                                  // Failed updating entryRef, retrying
                                  case false =>
                                    (counter1 + 1)
                                      .asLeft[Either[Int, (A, Option[F[Unit]])]]
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
                                .complete1(entry.asRight)
                                .flatMap {
                                  // We successfully completed the deferred, now trying to set the value.
                                  case true =>
                                    setRef(EntryState.Value(entry)).map {
                                      // We successfully replaced the entry with our value, so we are done.
                                      case true =>
                                        (a, none[F[Unit]])
                                          .asRight[Int]
                                          .asRight[Int]
                                      // Another fiber placed their new value (only Removed should be possible)
                                      // before us so we retry accessing the entry.
                                      case false =>
                                        (counter1 + 1)
                                          .asLeft[Either[Int, (A, Option[F[Unit]])]]
                                    }
                                  // Failed to complete the deferred, meaning someone else completed it, and will
                                  // now set the new value in the entryRef. Retrying the lookup.
                                  case false =>
                                    (counter1 + 1)
                                      .asLeft[Either[Int, (A, Option[F[Unit]])]]
                                      .pure[F]
                                }
                            // Noop decision, exiting
                            case (a, Directive.Ignore | Directive.Remove) =>
                              (a, none[F[Unit]])
                                .asRight[Int]
                                .asRight[Int]
                                .pure[F]
                          }

                        // Entry was just removed, it soon will be gone from the map.
                        case (EntryState.Removed, _) =>
                          f(None) match {
                            // We want to place the new value;
                            // Retrying the map lookup, expecting a different result for our key.
                            case (_, _: Directive.Put[F, V]) =>
                              (counter + 1)
                                .asLeft[(A, Option[F[Unit]])]
                                .asRight[Int]
                                .pure[F]
                            // Noop decision, exiting
                            case (a, Directive.Ignore | Directive.Remove) =>
                              (a, none[F[Unit]])
                                .asRight[Int]
                                .asRight[Int]
                                .pure[F]
                          }
                      }
                      .uncancelable
                  }
                }
            }
        }
      }

      def contains(key: K) = {
        ref
          .get
          .map { _.contains(key) }
      }


      def size = {
        ref
          .get
          .map { _.size }
      }


      def keys = {
        ref
          .get
          .map { _.keySet }
      }


      def values = {
        ref
          .get
          .flatMap { entryRefs =>
            entryRefs
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

      def values1 = {
        ref
          .get
          .flatMap { entryRefs =>
            entryRefs
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
        0.tailRecM { counter =>
          ref
            .access
            .flatMap { case (entryRefs, set) =>
              entryRefs
                .get(key)
                .fold {
                  none[V]
                    .pure[F]
                    .asRight[Int]
                    .pure[F]
                } { entryRef =>
                  set(entryRefs - key)
                    .flatMap {
                      case true =>
                        // We just removed the entry for the map, now we need to release it.
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
                                  fiber
                                    .join
                                    .asRight[Int]
                                }

                            // We removed a loading value, and the fiber that will complete it will also
                            // release that value, so there is nothing for us to return.
                            case _: EntryState.Loading[F, V] =>
                              none[V]
                                .pure[F]
                                .asRight[Int]
                                .pure[F]

                            // We removed an entry that was already being removed by another fiber, so we are done.
                            case EntryState.Removed =>
                              none[V]
                                .pure[F]
                                .asRight[Int]
                                .pure[F]
                          }
                      case false =>
                        (counter + 1)
                          .asLeft[F[Option[V]]]
                          .pure[F]
                    }
                    .uncancelable
                }
            }
        }
      }


      def clear = {
        ref
          .getAndSet(EntryRefs.empty)
          .flatMap { entryRefs =>
            entryRefs
              .parFoldMap1 { case (_, entryRef) =>
                entryRef
                  .getOption
                  .flatMap { _.foldMapM { _.release1 } }
                  .uncancelable
              }
              .start
          }
          .uncancelable
          .map { _.join }
      }

      def foldMap[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = {
        ref
          .get
          .flatMap { entryRefs =>
            val zero = CommutativeMonoid[A]
              .empty
              .pure[F]
            entryRefs.foldLeft(zero) { case (a, (key, entryRef)) =>
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

      def foldMapPar[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = {
        ref
          .get
          .flatMap { entryRefs =>
            Parallel[F].sequential {
              val zero = Parallel[F]
                .applicative
                .pure(CommutativeMonoid[A].empty)
              entryRefs
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
      def release1(implicit F: Monad[F]): F[Unit] = self.release.foldA
    }
  }

  sealed trait EntryState[+F[_], +A]
  object EntryState {
    final case class Loading[F[_], A](deferred: Deferred[F, Either[Throwable, Entry[F, A]]]) extends EntryState[F, A]
    final case class Value[F[_], A](entry: Entry[F, A]) extends EntryState[F, A]
    final case object Removed extends EntryState[Nothing, Nothing]
  }

  type DeferredThrow[F[_], A] = Deferred[F, Either[Throwable, A]]

  type EntryRef[F[_], A] = Ref[F, EntryState[F, A]]

  type EntryRefs[F[_], K, V] = Map[K, EntryRef[F, V]]

  object EntryRefs {
    def empty[F[_], K, V]: EntryRefs[F, K, V] = Map.empty
  }

  implicit class DeferredOps[F[_], A](val self: Deferred[F, A]) extends AnyVal {
    def complete1(a: A)(implicit F: MonadThrow[F]): F[Boolean] = {
      self
        .complete(a)
        .as(true)
        .handleError { _ => false }
    }
  }

  implicit class DeferredThrowOps[F[_], A](val self: DeferredThrow[F, A]) extends AnyVal {
    def getOrError(implicit F: MonadThrow[F]): F[A] = {
      self
        .get
        .flatMap {
          case Right(a) => a.pure[F]
          case Left(a)  => a.raiseError[F, A]
        }
    }

    def getOption(implicit F: Functor[F]): F[Option[A]] = {
      self
        .get
        .map { _.toOption }
    }
  }

  implicit class EntryStateOps[F[_], A](val self: EntryState[F, A]) extends AnyVal {

    def getOption(implicit F: Applicative[F]): F[Option[Entry[F, A]]] =
      self match {
        case EntryState.Loading(deferred) => deferred.getOption
        case EntryState.Value(entry) => entry.some.pure[F]
        case EntryState.Removed => none[Entry[F, A]].pure[F]
      }

    def optEither(implicit F: MonadThrow[F]): Option[Either[F[A], A]] =
      self match {
        case EntryState.Value(entry) =>
          entry
            .value
            .asRight[F[A]]
            .some
        case EntryState.Loading(deferred) =>
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

    def getOption(implicit F: Monad[F]): F[Option[Entry[F, A]]] = {
      self
        .get
        .flatMap(_.getOption)
    }

    def optEither(implicit F: MonadThrow[F]): F[Option[Either[F[A], A]]] = {
      self
        .get
        .map(_.optEither)
    }

    def value(implicit F: MonadThrow[F]): F[Option[F[A]]] = {
      self
        .get
        .map {
          case EntryState.Value(entry)   =>
            entry
              .value
              .pure[F]
              .some
          case EntryState.Loading(deferred) =>
            deferred
              .getOrError
              .map { _.value }
              .some
          case EntryState.Removed =>
            none[F[A]]
        }
    }

    def update1(f: A => A)(implicit F: Monad[F]): F[Unit] = {
      0.tailRecM { counter =>
        self
          .access
          .flatMap {
            case (EntryState.Value(entry), set) =>
              val entry1 = entry.copy(value = f(entry.value))
              set(EntryState.Value(entry1)).map {
                case true  => ().asRight[Int]
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

  implicit class Ops[F[_], A](val fa: F[A]) extends AnyVal {
    def race1[B](fb: F[B])(implicit F: Concurrent[F]): F[Either[A, (Fiber[F, A], B)]] = {
      fa
        .racePair(fb)
        .flatMap {
          case Left((a, fiber))  =>
            fiber
              .cancel
              .as(a.asLeft[(Fiber[F, A], B)])
          case Right((fiber, b)) =>
            (fiber, b)
              .asRight[A]
              .pure[F]
        }
    }
  }
}