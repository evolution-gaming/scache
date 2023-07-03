package com.evolution.scache

import cats.effect.Resource
import cats.effect.BracketThrow
import cats.syntax.all._

import scala.util.control.NoStackTrace
import cats.MonadThrow

/** Compat is needed because there is a bug in Scala3 compiler, see
  * https://github.com/lampepfl/dotty/issues/18099. Scala's 2 implementation
  * crashes Scalas's 3 compiler.
  * 
  * But this is Cats Effect 2 branch and here it is just for the sake of consistency in sources between branches.
  */
object CacheOpsCompat {
  private[scache] case object NoneError
      extends RuntimeException
      with NoStackTrace

  implicit class CacheXtensionCompat[F[_], K, V](val self: Cache[F, K, V])
      extends AnyVal {

    /** Gets a value for specific key, or tries to load it.
      *
      * The difference between this method and [[Cache#getOrUpdate1]] is that
      * this one allows the loading function to fail finding the value, i.e.
      * return [[scala.None]].
      *
      * Also this method is meant to be used where [[cats.effect.Resource]] is
      * not convenient to use, i.e. when integration with legacy code is
      * required or for internal implementation. For all other cases it is
      * recommended to use [[#getOrUpdateResourceOpt]] instead as more
      * human-readable alternative.
      *
      * @param key
      *   The key to return the value for.
      * @param value
      *   The function to run to load the missing value with.
      *
      * @tparam A
      *   Arbitrary type of a value to return in case key was not present in a
      *   cache.
      *
      * @return
      *   The same semantics applies as in [[Cache#getOrUpdate1]], except that
      *   the method may return [[scala.None]] in case `value` completes to
      *   [[scala.None]].
      */
    def getOrUpdateOpt1Compat[A](key: K)(
      value: => F[Option[(A, V, Option[Cache[F, K, V]#Release])]])(implicit
      F: MonadThrow[F]
    ): F[Option[Either[A, Either[F[V], V]]]] = {
      self
        .getOrUpdate1(key) {
          value.flatMap {
            case Some((a, value, release)) => (a, value, release).pure[F]
            case None                      => NoneError.raiseError[F, (A, V, Option[Cache[F, K, V]#Release])]
          }
        }
        .map { _.some }
        .recover { case NoneError => none }
    }

    /** Gets a value for specific key, or tries to load it.
      *
      * The difference between this method and [[#getOrUpdateResource]] is that
      * this one allows the loading function to fail finding the value,
      * i.e. return [[scala.None]].
      *
      * @param key
      *   The key to return the value for.
      * @param value
      *   The function to run to load the missing value with.
      *
      * @return
      *   The same semantics applies as in [[#getOrUpdateResource]], except that
      *   the method may return [[scala.None]] in case `value` completes to
      *   [[scala.None]]. The resource will be released normally even if `None`
      *   is returned.
      */
    def getOrUpdateResourceOptCompat(key: K)(value: => Resource[F, Option[V]])(implicit F: BracketThrow[F]): F[Option[V]] = {
      self
        .getOrUpdateOpt1(key) {
          value
            .allocated
            .flatMap {
              case (Some(a), release) =>
                (a, a, release.some)
                  .some
                  .pure[F]
              case (None, release)    =>
                release.as { none[(V, V, Option[F[Unit]])] }
            }
        }
        .flatMap {
          case Some(Right(Right(a))) => a.some.pure[F]
          case Some(Right(Left(a)))  => a.map { _.some }
          case Some(Left(a))         => a.some.pure[F]
          case None                  => none[V].pure[F]
        }
    }
  }

}
