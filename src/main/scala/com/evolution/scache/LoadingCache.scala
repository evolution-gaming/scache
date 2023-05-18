package com.evolution.scache

import cats.{Applicative, Functor, Monad, MonadThrow, Parallel}
import cats.effect.implicits.*
import cats.effect.{Concurrent, Deferred, Fiber, GenConcurrent, Outcome, Ref, Resource}
import cats.kernel.CommutativeMonoid
import com.evolutiongaming.catshelper.ParallelHelper.*
import cats.syntax.all.*


private[scache] object LoadingCache {

  def of[F[_] : Concurrent, K, V](
    map: EntryRefs[F, K, V],
  ): Resource[F, Cache[F, K, V]] = {
    for {
      ref   <- Ref[F].of(map).toResource
      cache <- of(ref)
    } yield cache
  }


  def of[F[_] : Concurrent, K, V](
    ref: Ref[F, EntryRefs[F, K, V]],
  ): Resource[F, Cache[F, K, V]] = {
    Resource.make {
      apply(ref).pure[F]
    } { cache =>
      cache.clear.flatten
    }
  }

  def apply[F[_]: Concurrent, K, V](
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

      import EntryState.*

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
                    case Value(entry)   =>
                      entry
                        .value
                        .some
                        .pure[F]
                    case Loading(deferred) =>
                      deferred
                        .get
                        .map { entry =>
                          entry
                            .toOption
                            .map { _.value }
                        }
                    case Removed(_) =>
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

      def getOrUpdate1[A](key: K)(value: => F[(A, V, Option[Release])]) = {
        0.tailRecM { counter =>
          ref
            .access
            .flatMap { case (entryRefs, set) =>
              entryRefs
                .get(key)
                .fold {
                  for {
                    deferred <- Deferred[F, Either[Throwable, Entry[F, V]]]
                    entryRef <- Ref[F].of[EntryState[F, V]](Loading(deferred))
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
                              // `value` got computed, and deferred was not completed by in another fiber in `put`
                              case Left(Right((a, entry))) =>
                                0.tailRecM { counter =>
                                  entryRef
                                    .access
                                    .flatMap {
                                      // Entry contains a computed value, which is only possible
                                      // if a successful `put` was performed by another fiber
                                      // after our `value` started computing.
                                      // That fiber also must've also completed our deferred,
                                      // so we don't do anything about it.
                                      // Returning their (newer) value, and releasing the value we just computed.
                                      case (Value(entry0), _) =>
                                        entry
                                          .release1
                                          .as {
                                            entry0
                                              .value
                                              .asRight[F[V]]
                                              .asRight[A]
                                              .asRight[Int]
                                          }

                                      // Our deferred is still there
                                      case (Loading(_), set) =>
                                        set(Value(entry)).flatMap {
                                          // Successfully replaced our deferred with the loaded value,
                                          // now we can complete it.
                                          case true  =>
                                            deferred
                                              .complete(entry.asRight)
                                              .flatMap {
                                                // Happy path: successfully completed our deferred
                                                case true  =>
                                                  a
                                                    .asLeft[Either[F[V], V]]
                                                    .pure[F]
                                                // Deferred got completed by another fiber,
                                                // so we return what they put there.
                                                // That fiber will take care of releasing the value we just computed
                                                // and already put in the entryRef (see `put` cycle).
                                                case false =>
                                                  deferred
                                                    .getOrError
                                                    .map { entry =>
                                                      entry
                                                        .value
                                                        .asRight[F[V]]
                                                        .asRight[A]
                                                    }
                                              }
                                              .map { _.asRight[Int] }
                                          // Our deferred got replaced by another fiber, retrying
                                          case false =>
                                            (counter + 1)
                                              .asLeft[Either[A, Either[F[V], V]]]
                                              .pure[F]
                                        }

                                      // The entry got removed by another fiber while we were loading it,
                                      // so we just complete the deferred with the value we computed:
                                      // 1: for anyone waiting on it in `get*`,
                                      // 2: so it can be released by the fiber that removed the entry (see `remove`).
                                      case (Removed(_), _) =>
                                        deferred
                                          .complete(entry.asRight)
                                          .flatMap {
                                            // Returning the value we successfully computed,
                                            // even though it got removed from the entryMap mid-evaluation.
                                            case true =>
                                              a
                                                .asLeft[Either[F[V], V]]
                                                .pure[F]
                                            // Another fiber completed our deferred (see `put`),
                                            // right before the entry was removed from the entryMap,
                                            // so we return what they `put` there, and release the value we just computed.
                                            // NB: the value we are returning might've already been released in `remove`.
                                            case false =>
                                              entry
                                                .release1
                                                .start
                                                .productR {
                                                  deferred
                                                    .getOrError
                                                    .map { entry =>
                                                      entry
                                                        .value
                                                        .asRight[F[V]]
                                                        .asRight[A]
                                                    }
                                                }
                                          }
                                          .map { _.asRight[Int] }
                                    }
                                }

                              // `value` computation completed with error,
                              // and deferred was not completed in another fiber in `put`.
                              case Left(Left(error)) =>
                                deferred
                                  .complete(error.asLeft)
                                  .flatMap {
                                    case true =>
                                      entryRef
                                        .get
                                        .flatMap {
                                          // Our deferred is still there
                                          case Loading(_) =>
                                            0.tailRecM { counter =>
                                              ref
                                                .access
                                                .flatMap { case (entryRefs, set) =>
                                                  entryRefs
                                                    .get(key)
                                                    .fold {
                                                      // Key was removed while we were loading,
                                                      // so we are just propagating the error
                                                      error.raiseError[F, Either[Int, V]]
                                                    } {
                                                      // The entry we added to the map is still there and unmodified,
                                                      // so we can safely remove it and propagate the error,
                                                      // without needing to release anything.
                                                      case `entryRef` =>
                                                        set(entryRefs - key).flatMap {
                                                          case true  =>
                                                            error.raiseError[F, Either[Int, V]]
                                                          case false =>
                                                            (counter + 1)
                                                              .asLeft[V]
                                                              .pure[F]
                                                        }
                                                      // Another fiber replaced the `ref` we added to the map,
                                                      // which shouldn't be possible, but if it happens,
                                                      // we return their value if it's already computed,
                                                      // or propagate our error if it's still loading,
                                                      // without waiting for their value to compute.
                                                      case entryRef =>
                                                        entryRef
                                                          .get
                                                          .flatMap {
                                                            case Loading(_) =>
                                                              error.raiseError[F, Either[Int, V]]
                                                            case Value(entry) =>
                                                              entry
                                                                .value
                                                                .asRight[Int]
                                                                .pure[F]
                                                            case Removed(_) =>
                                                              error.raiseError[F, Either[Int, V]]
                                                          }
                                                    }
                                                }
                                            }

                                          // Shouldn't be possible for us to complete `deferred` and then encounter
                                          // a different value already set,
                                          // but if it happens we just return that value and ignore the error we got.
                                          case Value(entry) =>
                                            entry
                                              .value
                                              .pure[F]

                                          // Key was removed while we were loading,
                                          // so we are just propagating the error
                                          case Removed(_) =>
                                            error.raiseError[F, V]
                                        }

                                    // Someone else completed the deferred before us, so they must've take care of
                                    // updating the `ref`, and we return their result.
                                    case false =>
                                      deferred
                                        .getOrError
                                        .map { _.value }
                                  }
                                  .map { value =>
                                    value
                                      .asRight[F[V]]
                                      .asRight[A]
                                  }

                              // Deferred was completed by `put` in another fiber before `value` computation completed.
                              // We return their value, and schedule release of the value that is still being computed.
                              case Right((fiber, entry)) =>
                                fiber
                                  .joinWithNever
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
                  entryRef
                    .optEither
                    .map {
                      case Some(either) =>
                        either
                          .asRight[A]
                          .asRight[Int]
                      case None =>
                        (counter + 1)
                          .asLeft[Either[A, Either[F[V], V]]]
                    }
                }
            }
        }
      }

      def put(key: K, value: V, release: Option[Release]) = {
        val entry = entryOf(value, release)
        0.tailRecM { counter =>
          ref
            .access
            .flatMap { case (entryRefs, set) =>
              entryRefs
                .get(key)
                .fold {
                  // No entry present in the map, so we add one
                  Ref[F]
                    .of[EntryState[F, V]](Value(entry))
                    .flatMap { entryRef =>
                      set(entryRefs.updated(key, entryRef)).map {
                        case true  =>
                          none[V]
                            .pure[F]
                            .asRight[Int]
                        case false =>
                          (counter + 1).asLeft[F[Option[V]]]
                      }
                    }
                } { entryRef =>
                  0.tailRecM { counter =>
                    entryRef
                      .access
                      .flatMap {
                        // A computed value is already present in the map, so we are replacing it, and taking care of
                        // releasing the old value.
                        case (Value(entry0), set) =>
                          set(Value(entry))
                            .flatMap {
                              case true  =>
                                entry0
                                  .release
                                  .traverse { _.start }
                                  .map { fiber =>
                                    fiber
                                      .foldMapM { _.joinWithNever }
                                      .as { entry0.value.some }
                                      .asRight[Int]
                                  }
                              case false =>
                                (counter + 1)
                                  .asLeft[F[Option[V]]]
                                  .pure[F]
                            }

                        // The value is still loading, so we complete the `deferred` with our value, and the creator of
                        // of it will take care of releasing their computed value when they see that it's been completed.
                        case (Loading(deferred), set) =>
                          deferred
                            .complete(entry.asRight)
                            .flatMap {
                              case true =>
                                set(Value(entry)).map {
                                  case true =>
                                    none[V]
                                      .pure[F]
                                      .asRight[Int]
                                  case false =>
                                    (counter + 1).asLeft[F[Option[V]]]
                                }
                              case false =>
                                (counter + 1)
                                  .asLeft[F[Option[V]]]
                                  .pure[F]
                            }

                        // The key was just removed from the map, so we retry from the beginning:
                        // at this point the key shouldn't be present in the map anymore.
                        case (Removed(_), _) =>
                          entry
                            .release
                            .traverse { _.start }
                            .as { none[V] }
                            .asRight[Int]
                            .pure[F]
                      }
                      .uncancelable
                      .map { _.asRight[Int] }
                  }
                }
            }
        }
      }

//      private def put1(key: K, value: V, release: Option[Release]): F[F[Option[V]]] =
//        modify[Unit](key)(_ => ((), Some((value, release)))).map { case (_, f) => f }

      // Modify the optionally present value associated with the given key.
      // If `f` returns `None` the entry is removed, otherwise the value is updated.
      // Returned outer effect contains the result of `f`,
      // and the inner effect represents the release function of the previously stored value.
//      def modify[A](key: K)(f: Option[V] => (A, Option[(V, Option[Release])])): F[(A, F[Option[V]])] = {
//
//        def tryUpdateEntry(
//          entry0: Entry[F, V],
//          set: Either[DeferredThrow[F, Entry[F, V]], Entry[F, V]] => F[Boolean],
//        ): F[Either[Unit, Option[V]]] =
//          f(entry0.value.some) match {
//            case (a, Some((newValue, newRelease))) =>
//              set(entryOf(newValue, newRelease).asRight).flatMap {
//                case true =>
//                  entry0
//                    .release
//                    .traverse { _.start }
//                    .map { fiber =>
//                      val m = fiber
//                        .foldMapM { _.joinWithNever }
//                        m.as { newValue.some }
//                        .asRight[Unit]
//                    }
//                case false =>
//                  none[V].asRight[Unit].pure[F]
//              }
//            case (a, None) =>
//              ???
//              ().asLeft[Option[V]].pure[F]
//          }
//
//        ().tailRecM { _ =>
//          ref
//            .access
//            .flatMap { case (entryRefs, setRefs) =>
//              entryRefs
//                .get(key)
//                .fold {
//                  f(None) match {
//                    case (a, Some((newValue, newRelease))) =>
//                      Ref[F]
//                        .of(entryOf(newValue, newRelease).asRight[DeferredThrow[F, Entry[F, V]]])
//                        .flatMap { entryRef =>
//                          setRefs(entryRefs.updated(key, entryRef)).map {
//                            case true =>
//                              (a, none[V].pure[F])
//                                .asRight[Unit]
//                            case false =>
//                              ().asLeft[F[(A, F[Option[V]])]]
//                          }
//                        }
//                    case (a, None) =>
//                      (a, none[V].pure[F])
//                        .asRight[Unit]
//                        .pure[F]
//                  }
//                } { entryRef =>
//                  // Entry exists for the given key, so we try to modify it
//                  ().tailRecM { _ =>
//                    entryRef
//                      .access
//                      .flatMap {
//                        // Entry contains already calculated value
//                        case (Right(entry0), set) =>
//                          tryUpdateEntry(entry0, set)
//                        // Entry contains deferred value
//                        case (Left(deferred), set) =>
//                          deferred
//                            .get
//                            .flatMap {
//                              case Left(_) =>
//                                // Deferred was completed with an error,
//                                // which means it will soon be removed or replaced, so we retry
//                                ().asLeft[Option[V]].pure[F]
//                              case Right(entry0) =>
//                                tryUpdateEntry(entry0, set)
//                            }
//                      }
//                      .uncancelable
//                      .map { _.asRight[Unit] }
//                  }
//                }
//            }
//        }
//      }

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


      def remove(key: K) = {
        ref
          .modify { entryRefs =>
            entryRefs
              .get(key)
              .fold {
                (entryRefs, none[EntryRef[F, V]])
              } { entryRef =>
                (entryRefs - key, entryRef.some)
              }
          }
          .flatMap { maybeRemovedEntryRef =>
            maybeRemovedEntryRef
              .traverse { entryRef =>
                entryRef
                  .getAndSet(Removed[F, V]())
                  .flatMap { previousEntryState =>
                    previousEntryState
                      .getOption
                      .flatMap { maybeEntry =>
                        maybeEntry
                          .traverse { entry =>
                            entry
                              .release1
                              .as {
                                entry.value
                              }
                          }
                      }
                      .start
                      .map {_.joinWithNever }
                  }
              }
              .map(_.getOrElse(none[V].pure[F]))
              .uncancelable
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
          .map { _.joinWithNever }
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

  final case class Entry[F[_], A](value: A, release: Option[F[Unit]])

  object Entry {
    implicit class EntryOps[F[_], A](val self: Entry[F, A]) extends AnyVal {
      def release1(implicit F: Monad[F]): F[Unit] = self.release.foldA
    }
  }

  sealed trait EntryState[F[_], A] {
    def getOption(implicit F: Applicative[F]): F[Option[Entry[F, A]]] =
      this match {
        case EntryState.Loading(deferred) => deferred.getOption
        case EntryState.Value(entry)      => entry.some.pure[F]
        case EntryState.Removed(_)        => none[Entry[F, A]].pure[F]
      }
  }

  object EntryState {
    final case class Loading[F[_], A](deferred: Deferred[F, Either[Throwable, Entry[F, A]]]) extends EntryState[F, A]
    final case class Value[F[_], A](entry: Entry[F, A]) extends EntryState[F, A]
    final case class Removed[F[_], A](stub: Boolean = true) extends EntryState[F, A]
  }

  type DeferredThrow[F[_], A] = Deferred[F, Either[Throwable, A]]

  type EntryRef[F[_], A] = Ref[F, EntryState[F, A]]

  type EntryRefs[F[_], K, V] = Map[K, EntryRef[F, V]]

  object EntryRefs {
    def empty[F[_], K, V]: EntryRefs[F, K, V] = Map.empty
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

  implicit class EntryRefOps[F[_], A](val self: EntryRef[F, A]) extends AnyVal {

    import EntryState.*

    def getOption(implicit F: Monad[F]): F[Option[Entry[F, A]]] = {
      self
        .get
        .flatMap(_.getOption)
    }

    def optEither(implicit F: MonadThrow[F]): F[Option[Either[F[A], A]]] = {
      self
        .get
        .map {
          case Value(entry)   =>
            entry
              .value
              .asRight[F[A]]
              .some
          case Loading(deferred) =>
            deferred
              .get
              .flatMap {
                case Right(entry) =>
                  entry
                    .value
                    .pure[F]
                case Left(error)  =>
                  error.raiseError[F, A]
              }
              .asLeft[A]
              .some
          case Removed(_) =>
            none[Either[F[A], A]]
        }
    }

    def value(implicit F: MonadThrow[F]): F[Option[F[A]]] = {
      self
        .get
        .map {
          case Value(entry)   =>
            entry
              .value
              .pure[F]
              .some
          case Loading(deferred) =>
            deferred
              .getOrError
              .map { _.value }
              .some
          case Removed(_) =>
            none[F[A]]
        }
    }

    def update1(f: A => A)(implicit F: Monad[F]): F[Unit] = {
      0.tailRecM { counter =>
        self
          .access
          .flatMap {
            case (Value(entry), set) =>
              val entry1 = entry.copy(value = f(entry.value))
              set(Value(entry1)).map {
                case true  => ().asRight[Int]
                case false => (counter + 1).asLeft[Unit]
              }
            case (Loading(_), _) =>
              ()
                .asRight[Int]
                .pure[F]
            case (Removed(_), _) =>
              ()
                .asRight[Int]
                .pure[F]
          }
      }
    }
  }

  implicit class Ops[F[_], A, E](val fa: F[A]) extends AnyVal {
    def race1[B](
      fb: F[B])(implicit
      F: GenConcurrent[F, E]
    ): F[Either[A, (Fiber[F, E, A], B)]] = {
      import F.*
      uncancelable { poll =>
        poll(racePair(fa, fb)).flatMap {
          case Left((a, fiber))  =>
            a match {
              case Outcome.Succeeded(a) =>
                fiber
                  .cancel
                  .productR { a }
                  .map { _.asLeft }
              case Outcome.Errored(a)   =>
                fiber
                  .cancel
                  .productR { raiseError(a) }
              case Outcome.Canceled()   =>
                poll(canceled) *> never
            }
          case Right((fiber, b)) =>
            b match {
              case Outcome.Succeeded(b) => b.map { b => (fiber, b).asRight[A] }
              case Outcome.Errored(eb)  => raiseError(eb)
              case Outcome.Canceled()   =>
                poll(fiber.join)
                  .onCancel(fiber.cancel)
                  .flatMap {
                    case Outcome.Succeeded(a) => a.map { _.asLeft[(Fiber[F, E, A], B)] }
                    case Outcome.Errored(a)   => raiseError(a)
                    case Outcome.Canceled()   => poll(canceled) *> never
                  }
            }
        }
      }
    }
  }
}