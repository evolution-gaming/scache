package com.evolution.scache

import cats.effect.Resource
import cats.syntax.all._
import cats.{Applicative, Functor, ~>}
import com.evolutiongaming.catshelper.BracketThrowable

@deprecated("do not use `Releasable` as well as relevant methods", "3.6.0")
final case class Releasable[F[_], A](value: A, release: F[Unit])

@deprecated("do not use `Releasable` as well as relevant methods", "3.6.0")
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


  @deprecated("use `functorReleasable1` instead", "3.5.0")
  def functorReleasable[F[_] : Applicative]: Functor[Releasable[F, *]] = new Functor[Releasable[F, *]] {

    def map[A, B](fa: Releasable[F, A])(f: A => B) = fa.map(f)
  }

  implicit def functorReleasable1[F[_]]: Functor[Releasable[F, *]] = new Functor[Releasable[F, *]] {

    def map[A, B](fa: Releasable[F, A])(f: A => B) = fa.map(f)
  }


  implicit class ReleasableOps[F[_], A](val self: Releasable[F, A]) extends AnyVal {

    def map[B](f: A => B): Releasable[F, B] = self.copy(value = f(self.value))
    
    def mapK[G[_]](f: F ~> G): Releasable[G, A] = self.copy(release = f(self.release))
  }
}