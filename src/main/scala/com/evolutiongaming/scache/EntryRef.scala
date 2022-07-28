package com.evolutiongaming.scache

import cats.Applicative
import cats.effect.{Concurrent, Deferred, Ref}
import cats.effect.implicits._
import cats.syntax.all._

trait EntryRef[F[_], A] {
  import EntryRef.Entry

  def get: F[A]

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

    def load(ref: Ref[F, Entry[F, A]]) = {
      value
        .attempt
        .flatMap {
          case Right(value) =>
            ref
              .modify {
                case entry: Entry.Loaded[F, A] =>
                  val release = value.release.combineAll
                  (entry, release)

                case entry: Entry.Loading[F, A] =>
                  val update = entry
                    .deferred
                    .complete(value.asRight)
                    .ifM(Applicative[F].unit, value.release.combineAll)
                  (value, update)
              }
              .flatten

          case Left(error) =>
            ref
              .get
              .flatMap {
                case _    : Entry.Loaded[F, A]      => ().pure[F]
                case entry: Entry.Loading[F, A] =>
                  cleanup.flatMap { _ =>
                    entry
                      .deferred
                      .complete(error.asLeft)
                      .void
                  }
              }
        }
    }

    for {
      deferred <- Deferred[F, Either[Throwable, Entry.Loaded[F, A]]]
      ref      <- Ref.of[F, Entry[F, A]](Entry.Loading(deferred))
    } yield {

      val value = for {
        _      <- load(ref).start
        loaded <- deferred.get
        loaded <- loaded.liftTo[F]
      } yield {
        loaded.value
      }

      (apply(ref), value)
    }
  }


  def apply[F[_]: Concurrent, A](self: Ref[F, Entry[F, A]]): EntryRef[F, A] = {

    implicit def monoidUnit = Applicative.monoid[F, Unit]
    
    def loaded = self.get.flatMap {
      case entry: Entry.Loaded[F, A]  => entry.pure[F]
      case entry: Entry.Loading[F, A] => entry.deferred.get.flatMap(_.liftTo[F])
    }

    new EntryRef[F, A] {

      def get = loaded.map(_.value)

      def getLoaded = self.get.map {
        case entry: Entry.Loaded[F, A]  => entry.value.some
        case _    : Entry.Loading[F, A] => none[A]
      }

      def release = loaded.flatMap(_.release.combineAll)

      def updateLoaded(f: A => A): F[Unit] = {
        self.update {
          case entry: Entry.Loaded[F, A]  => entry.copy(value = f(entry.value))
          case entry: Entry.Loading[F, A] => entry
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
