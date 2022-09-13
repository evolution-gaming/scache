package com.evolutiongaming.scache

import cats.{Applicative, Monoid}
import cats.effect.{Concurrent, ExitCase}
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.syntax.all._
import cats.syntax.all._

trait EntryRef[F[_], A] {
  import EntryRef.Entry

  def get: F[A]

  def get1: F[Either[F[A], A]]

  def getLoaded: F[Option[A]]

  def release: F[Unit]

  def updateLoaded(f: A => A): F[Unit]

  def put(loaded: Entry.Loaded[F, A]): F[F[Option[A]]]
}

object EntryRef {

  def loaded[F[_]: Concurrent, A](entry: Entry.Loaded[F, A]): F[EntryRef[F, A]] = {
    Ref
      .of[F, Entry[F, A]](entry)
      .map { entryRef => apply(entryRef) }
  }


  def loading[F[_]: Concurrent, A](
    value: => F[Entry.Loaded[F, A]],
    cleanup: F[Unit]
  ): F[(EntryRef[F, A], F[A])] = {

    implicit def monoidUnit = Applicative.monoid[F, Unit]

    for {
      deferred <- Deferred[F, Either[Throwable, Entry.Loaded[F, A]]]
      ref      <- Ref.of[F, Entry[F, A]](Entry.Loading(deferred))
    } yield {

      def fail(error: Throwable) = {
        ref
          .get
          .flatMap {
            case _: Entry.Loaded[F, A]  =>
              ().pure[F]
            case a: Entry.Loading[F, A] =>
              if (a.deferred == deferred) {
                cleanup
              } else {
                ().pure[F]
              }
          }
          .flatMap { _ =>
            deferred
              .complete(error.asLeft)
              .handleError { _ => () }
          }
      }

      val load = value
        .guaranteeCase {
          case ExitCase.Completed =>
            ().pure[F]
          case ExitCase.Error(a)  =>
            fail(a)
          case ExitCase.Canceled  =>
            fail(CancelledError)
        }
        .flatMap { value =>
          def release = value
            .release
            .combineAll

          deferred
            .complete(value.asRight)
            .attempt
            .flatMap {
              case Right(()) =>
                0.tailRecM { counter =>
                  ref
                    .access
                    .flatMap { case (entry, set) =>
                      entry match {
                        case _: Entry.Loaded[F, A]  =>
                          release.map { _.asRight[Int] }
                        case a: Entry.Loading[F, A] =>
                          if (a.deferred == deferred) {
                            set(value).map {
                              case true  => ().asRight[Int]
                              case false => (counter + 1).asLeft[Unit]
                            }
                          } else {
                            release.map { _.asRight[Int] }
                          }
                      }
                    }
                }
              case Left(_)   =>
                release
            }
        }
        .start
        .productR {
          deferred
            .get
            .rethrow
            .map { _.value }
        }

      (apply(ref), load)
    }
  }


  def apply[F[_]: Concurrent, A](self: Ref[F, Entry[F, A]]): EntryRef[F, A] = {

    implicit def monoidUnit: Monoid[F[Unit]] = Applicative.monoid[F, Unit]

    new EntryRef[F, A] {

      def get = {
        self
          .get
          .flatMap {
            case a: Entry.Loaded[F, A]  =>
              a
                .value
                .pure[F]
            case a: Entry.Loading[F, A] =>
              a
                .deferred
                .get
                .flatMap { _.liftTo[F] }
                .map { _.value }
          }
      }

      def get1 = {
        self
          .get
          .map {
            case a: Entry.Loaded[F, A]  =>
              a
                .value
                .asRight[F[A]]
            case a: Entry.Loading[F, A] =>
              a
                .deferred
                .get
                .flatMap { a =>
                  a
                    .liftTo[F]
                    .map { _.value }
                }
                .asLeft[A]
          }
      }

      def getLoaded = {
        self
          .get
          .map {
            case entry: Entry.Loaded[F, A] => entry.value.some
            case _: Entry.Loading[F, A]    => none[A]
          }
      }

      def release = {
        self
          .get
          .flatMap {
            case a: Entry.Loaded[F, A]  =>
              a
                .release
                .pure[F]
            case a: Entry.Loading[F, A] =>
              a
                .deferred
                .get
                .flatMap { _.liftTo[F] }
                .map { _.release }
          }
          .flatMap { _.combineAll }
      }

      def updateLoaded(f: A => A): F[Unit] = {
        self.update {
          case a: Entry.Loaded[F, A]  => a.copy(value = f(a.value))
          case a: Entry.Loading[F, A] => a
        }
      }

      def put(loaded: Entry.Loaded[F, A]) = {

        def onLoaded(entry: Entry.Loaded[F, A]) = for {
          fiber <- entry.release.traverse(_.start)
        } yield {
          fiber.traverse(_.join) as entry.value.some
        }

        def onLoading(deferred: Deferred[F, Either[Throwable, Entry.Loaded[F, A]]]) = {

          def onConflict = for {
            loaded <- deferred.get
            value  <- loaded.fold((_: Throwable) => none[A].pure[F].pure[F], onLoaded)
          } yield value

          deferred
            .complete(loaded.asRight)
            .redeemWith((_: Throwable) => onConflict, _ => none[A].pure[F].pure[F])
        }

        self
          .getAndSet(loaded)
          .flatMap {
            case entry: Entry.Loaded[F, A]  => onLoaded(entry)
            case entry: Entry.Loading[F, A] => onLoading(entry.deferred)
          }
          .uncancelable
      }
    }
  }


  sealed trait Entry[F[_], A] extends Product

  object Entry {

    final case class Loaded[F[_], A](value: A, release: Option[F[Unit]]) extends Entry[F, A]

    final case class Loading[F[_], A](deferred: Deferred[F, Either[Throwable, Loaded[F, A]]]) extends Entry[F, A]
  }
}
