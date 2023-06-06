package com.evolution.scache

import cats.effect.{Resource, Temporal}
import cats.kernel.CommutativeMonoid
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{MeasureDuration, Schedule}
import com.evolutiongaming.smetrics

import scala.concurrent.duration.*

object CacheMetered {

  @deprecated("Use `apply1` instead", "4.5.0")
  def apply[F[_]: smetrics.MeasureDuration: Temporal, K, V](
    cache: Cache[F, K, V],
    metrics: CacheMetrics[F],
    interval: FiniteDuration = 1.minute
  ): Resource[F, Cache[F, K, V]] = {
    implicit val md: MeasureDuration[F] = smetrics.MeasureDuration[F].toCatsHelper
    apply1(cache, metrics, interval)
  }

  def apply1[F[_]: MeasureDuration: Temporal, K, V](
    cache: Cache[F, K, V],
    metrics: CacheMetrics[F],
    interval: FiniteDuration = 1.minute
  ): Resource[F, Cache[F, K, V]] = {

    def measureSize = {
      for {
        size <- cache.size
        _    <- metrics.size(size)
      } yield {}
    }

    def releaseMetered(duration: F[FiniteDuration], release: F[Unit]) = {
      for {
        d <- duration
        _ <- metrics.life(d)
        a <- release
      } yield a
    }

    for {
      _ <- Schedule(interval, interval)(measureSize)
    } yield {
      abstract class CacheMetered extends Cache.Abstract1[F, K, V]
      new CacheMetered {

        def get(key: K) = {
          for {
            a <- cache.get(key)
            _ <- metrics.get(a.isDefined)
          } yield a
        }

        def get1(key: K) = {
          for {
            a <- cache.get1(key)
            _ <- metrics.get(a.isDefined)
          } yield a
        }

        def getOrUpdate(key: K)(value: => F[V]) = {
          getOrUpdate1(key) { value.map { a => (a, a, none[Release]) } }
            .flatMap {
              case Right(Right(a)) => a.pure[F]
              case Right(Left(a))  => a
              case Left(a)         => a.pure[F]
            }
        }

        def getOrUpdate1[A](key: K)(value: => F[(A, V, Option[Release])]) = {
          for {
            result <- cache.getOrUpdate1(key) {
              for {
                _          <- metrics.get(false)
                start      <- MeasureDuration[F].start
                value      <- value.attempt
                duration   <- start
                loadSucceed = value match {
                  case Right(_) | Left(Cache.NoneError) => true
                  case Left(_)                          => false
                }
                _     <- metrics.load(duration, loadSucceed)
                value <- value.liftTo[F]
              } yield {
                val (a, v, release) = value
                val release1 = releaseMetered(start, release.getOrElse { ().pure[F] })
                (a, v, release1.some) // TODO is this a good idea to convert option to always some?
              }
            }
            _ <- metrics.get(true).whenA(result.isRight)
          } yield result
        }

        def put(key: K, value: V, release: Option[Release]) = {
          for {
            duration <- MeasureDuration[F].start
            _        <- metrics.put
            release1  = releaseMetered(duration, release.getOrElse { ().pure[F] })
            value    <- cache.put(key, value, release1.some)
          } yield value
        }

        def contains(key: K) = cache.contains(key)

        def size = {
          for {
            d <- MeasureDuration[F].start
            a <- cache.size
            d <- d
            _ <- metrics.size(d)
          } yield a
        }

        def keys = {
          for {
            d <- MeasureDuration[F].start
            a <- cache.keys
            d <- d
            _ <- metrics.keys(d)
          } yield a
        }

        def values = {
          for {
            d <- MeasureDuration[F].start
            a <- cache.values
            d <- d
            _ <- metrics.values(d)
          } yield a
        }

        def values1 = {
          for {
            d <- MeasureDuration[F].start
            a <- cache.values1
            d <- d
            _ <- metrics.values(d)
          } yield a
        }

        def remove(key: K) = cache.remove(key)

        def clear = {
          for {
            d <- MeasureDuration[F].start
            a <- cache.clear
            d <- d
            _ <- metrics.clear(d)
          } yield a
        }

        def foldMap[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = {
          for {
            d <- MeasureDuration[F].start
            a <- cache.foldMap(f)
            d <- d
            _ <- metrics.foldMap(d)
          } yield a
        }

        def foldMapPar[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = {
          for {
            d <- MeasureDuration[F].start
            a <- cache.foldMapPar(f)
            d <- d
            _ <- metrics.foldMap(d)
          } yield a
        }
      }
    }
  }
}