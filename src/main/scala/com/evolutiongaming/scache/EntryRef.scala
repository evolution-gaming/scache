package com.evolutiongaming.scache

import cats.Applicative
import cats.effect.{Concurrent, Deferred, Ref}
import cats.effect.implicits.*
import cats.effect.kernel.Outcome
import cats.syntax.all.*
import cats.kernel.Monoid

trait EntryRef[F[_], A] {

  def get: F[A]

  def get1: F[Either[F[A], A]]

  def release: F[Unit]

  def update(f: A => A): F[Unit]

  def set(entry: EntryRef.Entry[F, A]): F[F[Option[A]]]
}

object EntryRef {

  def loaded[F[_]: Concurrent, A](entry: Entry[F, A]): F[EntryRef[F, A]] = {
    Ref[F]
      .of(entry.asRight[Deferred[F, Either[Throwable, Entry[F, A]]]])
      .map { ref => main(ref) }
  }


  def loading[F[_]: Concurrent, A](
    value: => F[Entry[F, A]],
    cleanup: F[Unit]
  ): F[(EntryRef[F, A], F[A])] = {

    implicit def monoidUnit: Monoid[F[Unit]] = Applicative.monoid[F, Unit]

    for {
      deferred <- Deferred[F, Either[Throwable, Entry[F, A]]]
      ref      <- Ref[F].of(deferred.asLeft[Entry[F, A]])
    } yield {

      def fail(error: Throwable) = {
        ref
          .get
          .flatMap {
            case Right(_)         => ().pure[F]
            case Left(`deferred`) => cleanup
            case Left(_)          => ().pure[F]
          }
          .flatMap { _ =>
            deferred
              .complete(error.asLeft)
              .void
          }
      }

      val load = value
        .guaranteeCase {
          case Outcome.Succeeded(a) =>
            a.flatMap { value =>

              def release = value
                .release
                .combineAll

              deferred
                .complete(value.asRight)
                .flatMap {
                  case true  =>
                    0.tailRecM { counter =>
                      ref
                        .access
                        .flatMap { case (entry, set) =>
                          entry match {
                            case Right(_)         =>
                              release.map { _.asRight[Int] }
                            case Left(`deferred`) =>
                              set(value.asRight).map {
                                case true  => ().asRight[Int]
                                case false => (counter + 1).asLeft[Unit]
                              }
                            case Left(_)          =>
                              release.map { _.asRight[Int] }
                          }
                        }
                    }
                  case false =>
                    release
                }
            }

          case Outcome.Errored(a) =>
            fail(a)
          case Outcome.Canceled() =>
            fail(CancelledError)
        }
        .start
        .productR {
          deferred
            .get
            .rethrow
            .map { _.value }
        }

      (main(ref), load)
    }
  }


  private def main[F[_]: Concurrent, A](
    ref: Ref[F, Either[Deferred[F, Either[Throwable, Entry[F, A]]], Entry[F, A]]]
  ): EntryRef[F, A] = {

    implicit def monoidUnit: Monoid[F[Unit]] = Applicative.monoid[F, Unit]

    class Main
    new Main with EntryRef[F, A] {

      def get = {
        ref
          .get
          .flatMap {
            case Right(a) =>
              a
                .value
                .pure[F]
            case Left(a)  =>
              a
                .get
                .flatMap { _.liftTo[F] }
                .map { _.value }
          }
      }

      def get1 = {
        ref
          .get
          .map {
            case Right(a) =>
              a
                .value
                .asRight[F[A]]
            case Left(a)  =>
              a
                .get
                .flatMap { _.liftTo[F] }
                .map { _.value }
                .asLeft[A]
          }
      }

      def release = {
        ref
          .get
          .flatMap {
            case Right(a) =>
              a
                .release
                .pure[F]
            case Left(a)  =>
              a
                .get
                .map {
                  case Right(a) => a.release
                  case Left(_)  => none[F[Unit]]
                }
          }
          .flatMap { _.combineAll }
      }

      def update(f: A => A): F[Unit] = {
        0.tailRecM { counter =>
          ref
            .access
            .flatMap {
              case (Right(state), set) =>
                val state1 = state.copy(value = f(state.value))
                set(state1.asRight).map {
                  case true  => ().asRight[Int]
                  case false => (counter + 1).asLeft[Unit]
                }
              case _                   =>
                ()
                  .asRight[Int]
                  .pure[F]
            }
        }
      }

      def set(entry: EntryRef.Entry[F, A]) = {

        def release1(entry: Entry[F, A]) = {
          entry
            .release
            .traverse { _.start }
            .map { fiber =>
              fiber
                .traverse { _.join }
                .as { entry.value.some }
            }
        }

        ref
          .getAndSet(entry.asRight)
          .flatMap {
            case Right(a)       =>
              release1(a)
            case Left(deferred) =>
              deferred
                .complete(entry.asRight)
                .flatMap {
                  case true  =>
                    none[A]
                      .pure[F]
                      .pure[F]
                  case false =>
                    deferred
                      .get
                      .flatMap {
                        case Right(a) =>
                          release1(a)
                        case Left(_)  =>
                          none[A]
                            .pure[F]
                            .pure[F]
                      }
                }
          }
          .uncancelable
      }
    }
  }

  final case class Entry[F[_], A](value: A, release: Option[F[Unit]])
}
