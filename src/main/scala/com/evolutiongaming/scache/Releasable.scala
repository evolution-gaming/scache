package com.evolutiongaming.scache

import cats.effect.Resource
import cats.syntax.all._
import cats.{Applicative, Functor, ~>}
import com.evolutiongaming.catshelper.BracketThrowable
import scala.annotation.nowarn

final case class Releasable[F[_], A](value: A, release: F[Unit])

object Releasable {

  def pure[F[_] : Applicative, A](value: A): Releasable[F, A] = Releasable(value, ().pure[F])

  def apply[F[_]](implicit F: Applicative[F]): ApplyBuilders[F] = new ApplyBuilders(F)

  def of[F[_] : BracketThrowable, A](resource: Resource[F, A]): F[Releasable[F, A]] = {
    for {
      ab <- resource.allocated
    } yield {
      val (value, release) = ab
      Releasable(value, release)
    }
  }


  final class ApplyBuilders[F[_]](val F: Applicative[F]) extends AnyVal {

    def pure[A](value: A): Releasable[F, A] = Releasable.pure[F, A](value)(F)
  }


  @deprecated(message = "use releaseFromFunctor which doesn't require Applicative instance", since = "4.0.1")
  @nowarn("msg=parameter value evidence$3 in method functorReleasable is never used")
  def functorReleasable[F[_]: Applicative]: Functor[Releasable[F, _]] = new Functor[Releasable[F, _]] {

    def map[A, B](fa: Releasable[F, A])(f: A => B) = fa.map(f)
  }

  implicit def releaseFromFunctor[F[_]]: Functor[Releasable[F, _]] = new Functor[Releasable[F, _]] {

    def map[A, B](fa: Releasable[F, A])(f: A => B) = fa.map(f)
  }

  implicit class ReleasableOps[F[_], A](val self: Releasable[F, A]) extends AnyVal {

    def map[B](f: A => B): Releasable[F, B] = self.copy(value = f(self.value))
    
    def mapK[G[_]](f: F ~> G): Releasable[G, A] = self.copy(release = f(self.release))
  }
}