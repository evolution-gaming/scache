package com.evolutiongaming.scache

import cats.Parallel
import cats.effect.implicits.*
import cats.effect.{Concurrent, Ref, Resource}
import cats.kernel.CommutativeMonoid
import com.evolutiongaming.catshelper.ParallelHelper.*
import cats.syntax.all.*

import scala.util.control.NoStackTrace


object LoadingCache {

  private sealed abstract class LoadingCache

  private[scache] def of[F[_] : Concurrent, K, V](
    map: EntryRefs[F, K, V],
  ): Resource[F, Cache[F, K, V]] = {
    for {
      ref   <- Ref[F].of(map).toResource
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

    val ignoreThrowable = (_: Throwable) => ()

    case object NoneError extends RuntimeException with NoStackTrace

    def entryOf(value: V, release: Option[F[Unit]]) = {
      EntryRef.Entry(
        value,
        release.map { _.handleError(ignoreThrowable) })
    }

    def put1(key: K, entry: EntryRef.Entry[F, V]) = {
      0.tailRecM { counter =>
        ref
          .access
          .flatMap { case (entries, set) =>
            entries
              .get(key)
              .fold {
                EntryRef
                  .loaded(entry)
                  .flatMap { entry =>
                    set(entries.updated(key, entry)).map {
                      case true  => none[V].pure[F].asRight[Int]
                      case false => (counter + 1).asLeft[F[Option[V]]]
                    }
                  }
              } { entryRef =>
                entryRef
                  .set(entry)
                  .map { _.asRight[Int] }
              }
          }
      }
    }

    def getOrUpdateReleasable1(key: K)(loaded: => F[EntryRef.Entry[F, V]]) = {
      0.tailRecM { counter =>
        ref
          .access
          .flatMap { case (entries, set) =>
            entries
              .get(key)
              .fold {
                val cleanup = ref.update { _ - key }
                EntryRef
                  .loading(loaded, cleanup)
                  .flatMap { case (entry, load) =>
                    set(entries.updated(key, entry))
                      .flatMap {
                        case true  => load.map { _.asRight[Int] }
                        case false => (counter + 1).asLeft[V].pure[F]
                      }
                      .uncancelable
                  }
              } { entry =>
                entry
                  .get
                  .map { _.asRight[Int] }
              }
          }
      }
    }

    new LoadingCache with Cache[F, K, V] {

      def get(key: K) = {
        ref
          .get
          .flatMap { entries =>
            entries
              .get(key)
              .fold {
                none[V].pure[F]
              } { entry =>
                entry
                  .get1
                  .flatMap {
                    case Right(a) =>
                      a
                        .some
                        .pure[F]
                    case Left(a)  =>
                      a
                        .map { _.some }
                        .handleError { _ => none }
                  }

              }
          }
      }

      def get1(key: K) = {
        ref
          .get
          .flatMap { entries =>
            entries
              .get(key)
              .fold {
                none[Either[F[V], V]].pure[F]
              } { entry =>
                entry
                  .get1
                  .map { _.some }
              }
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
        getOrUpdateReleasable1(key) {
          value.map { entryOf(_, none) }
        }
      }

      def getOrUpdateOpt(key: K)(value: => F[Option[V]]) = {
        getOrUpdateReleasable1(key) {
          for {
            value <- value
            value <- value.fold { NoneError.raiseError[F, V] } { _.pure[F] }
          } yield {
            entryOf(value, none)
          }
        }
          .map { _.some }
          .recover { case NoneError => none }
      }

      def getOrUpdateReleasable(key: K)(value: => F[Releasable[F, V]]) = {
        getOrUpdateReleasable1(key) {
          value.map { value => entryOf(value.value, value.release.some) }
        }
      }

      def getOrUpdateReleasableOpt(key: K)(value: => F[Option[Releasable[F, V]]]) = {
        getOrUpdateReleasable1(key) {
          for {
            value <- value
            value <- value.fold { NoneError.raiseError[F, Releasable[F, V]] } { _.pure[F] }
          } yield {
            entryOf(value.value, value.release.some)
          }
        }
          .map { _.some }
          .recover { case NoneError => none }
      }


      def put(key: K, value: V) = {
        def loaded = entryOf(value, none)
        put1(key, loaded)
      }


      def put(key: K, value: V, release: F[Unit]) = {
        def loaded = entryOf(value, release.some)
        put1(key, loaded)
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
          .map { entries =>
            entries.map { case (k, v) => (k, v.get) }
          }
      }

      def values1 = {
        ref
          .get
          .flatMap { entries =>
            entries
              .toList
              .traverse { case (k, v) =>
                v
                  .get1
                  .map { v => (k, v) }
              }
              .map { _.toMap }
          }
      }


      def remove(key: K) = {
        0.tailRecM { counter =>
          ref
            .access
            .flatMap { case (entries, set) =>
              entries
                .get(key)
                .fold {
                  none[V]
                    .pure[F]
                    .asRight[Int]
                    .pure[F]
                } { entry =>
                  set(entries - key)
                    .flatMap {
                      case true  =>
                        entry
                          .release
                          .flatMap { _ =>
                            entry
                              .get
                              .map { _.some }
                          }
                          .handleError { _ => none[V] }
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
              .values
              .parFoldMapTraversable { _.release.uncancelable }
              .start
          }
          .uncancelable
          .map { _.joinWithNever }
      }

      def foldMap[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = {
        ref
          .get
          .flatMap { entries =>
            val zero = CommutativeMonoid[A]
              .empty
              .pure[F]
            entries.foldLeft(zero) { case (a, (k, v)) =>
              for {
                a <- a
                v <- v.get1
                b <- f(k, v)
              } yield {
                CommutativeMonoid[A].combine(a, b)
              }
            }
          }
      }

      def foldMapPar[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = {
        ref
          .get
          .flatMap { entries =>
            Parallel[F].sequential {
              val zero = Parallel[F]
                .applicative
                .pure(CommutativeMonoid[A].empty)
              entries
                .foldLeft(zero) { case (a, (k, v)) =>
                  val b = Parallel[F].parallel {
                    for {
                      v <- v.get1
                      b <- f(k, v)
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


  type EntryRefs[F[_], K, V] = Map[K, EntryRef[F, V]]

  object EntryRefs {
    def empty[F[_], K, V]: EntryRefs[F, K, V] = Map.empty
  }
}