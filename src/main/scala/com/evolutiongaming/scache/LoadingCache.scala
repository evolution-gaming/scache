package com.evolutiongaming.scache

import cats.{Functor, Monad, MonadThrow, Parallel}
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
                    case Right(entry)   =>
                      entry
                        .value
                        .some
                        .pure[F]
                    case Left(deferred) =>
                      deferred
                        .get
                        .map { entry =>
                          entry
                            .toOption
                            .map { _.value }
                        }
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
              .traverse { _.either }
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
                    entryRef <- Ref[F].of(deferred.asLeft[Entry[F, V]])
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
                              case Left(Right((a, entry))) =>
                                0.tailRecM { counter =>
                                  entryRef
                                    .access
                                    .flatMap {
                                      case (Right(entry0), _) =>
                                        entry
                                          .release1
                                          .as {
                                            entry0
                                              .value
                                              .asRight[F[V]]
                                              .asRight[A]
                                              .asRight[Int]
                                          }

                                      case (Left(_), set) =>
                                        set(entry.asRight).flatMap {
                                          case true  =>
                                            deferred
                                              .complete(entry.asRight)
                                              .flatMap {
                                                case true  =>
                                                  a
                                                    .asLeft[Either[F[V], V]]
                                                    .pure[F]
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
                                          case false =>
                                            (counter + 1)
                                              .asLeft[Either[A, Either[F[V], V]]]
                                              .pure[F]
                                        }
                                    }
                                }

                              case Left(Left(error)) =>
                                deferred
                                  .complete(error.asLeft)
                                  .flatMap {
                                    case true =>
                                      entryRef
                                        .get
                                        .flatMap {
                                          case Left(_)      =>
                                            0.tailRecM { counter =>
                                              ref
                                                .access
                                                .flatMap { case (entryRefs, set) =>
                                                  entryRefs
                                                    .get(key)
                                                    .fold {
                                                      error.raiseError[F, Either[Int, V]]
                                                    } {
                                                      case `entryRef` =>
                                                        set(entryRefs - key).flatMap {
                                                          case true  =>
                                                            error.raiseError[F, Either[Int, V]]
                                                          case false =>
                                                            (counter + 1)
                                                              .asLeft[V]
                                                              .pure[F]
                                                        }
                                                      case entryRef   =>
                                                        entryRef
                                                          .get
                                                          .flatMap {
                                                            case Left(_)      =>
                                                              error.raiseError[F, Either[Int, V]]
                                                            case Right(entry) =>
                                                              entry
                                                                .value
                                                                .asRight[Int]
                                                                .pure[F]
                                                          }
                                                    }
                                                }
                                            }
                                          case Right(entry) =>
                                            entry
                                              .value
                                              .pure[F]
                                        }

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
                    .either
                    .map { value =>
                      value
                        .asRight[A]
                        .asRight[Int]
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
                  Ref[F]
                    .of(entry.asRight[DeferredThrow[F, Entry[F, V]]])
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
                        case (Right(entry0), set) =>
                          set(entry.asRight)
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

                        case (Left(deferred), set) =>
                          deferred
                            .complete(entry.asRight)
                            .flatMap {
                              case true  =>
                                set(entry.asRight).map {
                                  case true  =>
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
                      }
                      .uncancelable
                      .map { _.asRight[Int] }
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
                    .map { value => (key, value) :: values }
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
                    .either
                    .map { value => (key, value) :: values }
                }
              }
          }
          .map { _.toMap }
      }

      def readyValues: F[Map[K, V]] =
        ref
          .get
          .flatMap { entryRefs =>
            entryRefs.toVector.traverseFilter { case (key, entryRef) =>
              entryRef.get.map {
                case Left(_) => none // value is not loaded yet
                case Right(entry) => (key -> entry.value).some
              }
            }
          }
          .map(_.toMap)


      def remove(key: K) = {
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
                      case true  =>
                        entryRef
                          .getOption
                          .flatMap { entry =>
                            entry.traverse { entry =>
                              entry
                                .release1
                                .as { entry.value }
                            }
                          }
                          .start
                          .map { fiber =>
                            fiber
                              .joinWithNever
                              .asRight[Int]
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
                v <- entryRef.either
                b <- f(key, v)
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
                      v <- entryRef.either
                      b <- f(key, v)
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

  type DeferredThrow[F[_], A] = Deferred[F, Either[Throwable, A]]

  type EntryRef[F[_], A] = Ref[F, Either[DeferredThrow[F, Entry[F, A]], Entry[F, A]]]

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

    def getOption(implicit F: Monad[F]): F[Option[Entry[F, A]]] = {
      self
        .get
        .flatMap {
          case Right(a) => a.some.pure[F]
          case Left(a)  => a.getOption
        }
    }

    def either(implicit F: MonadThrow[F]): F[Either[F[A], A]] = {
      self
        .get
        .map {
          case Right(entry)   =>
            entry
              .value
              .asRight[F[A]]
          case Left(deferred) =>
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
        }
    }

    def value(implicit F: MonadThrow[F]): F[F[A]] = {
      self
        .get
        .map {
          case Right(entry)   =>
            entry
              .value
              .pure[F]
          case Left(deferred) =>
            deferred
              .getOrError
              .map { _.value }
        }
    }

    def update1(f: A => A)(implicit F: Monad[F]): F[Unit] = {
      0.tailRecM { counter =>
        self
          .access
          .flatMap {
            case (Right(entry), set) =>
              val entry1 = entry.copy(value = f(entry.value))
              set(entry1.asRight).map {
                case true  => ().asRight[Int]
                case false => (counter + 1).asLeft[Unit]
              }
            case (Left(_), _)        =>
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