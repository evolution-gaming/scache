package com.evolutiongaming.scache

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Resource, Timer}
import cats.kernel.CommutativeMonoid
import cats.syntax.all._
import com.evolutiongaming.catshelper.Schedule
import com.evolutiongaming.smetrics.MeasureDuration

import scala.concurrent.duration._

object CacheMetered {

  private sealed abstract class CacheMetered

  def apply[F[_] : Concurrent : Timer : MeasureDuration, K, V](
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

    val unit = ().pure[F]

    for {
      _ <- Schedule(interval, interval)(measureSize)
    } yield {

      new CacheMetered with Cache[F, K, V] {

        def get(key: K) = {
          for {
            value <- cache.get(key)
            _     <- metrics.get(value.isDefined)
          } yield value
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
          getOrUpdateReleasable(key) { 
            for {
              value <- value
            } yield {
              Releasable(value, unit)
            }
          }
        }

        def getOrUpdateOpt(key: K)(value: => F[Option[V]]) = {
          getOrUpdateReleasableOpt(key) {
            for {
              value <- value
            } yield for {
              value <- value
            } yield {
              Releasable(value, unit)
            }
          }
        }

        def getOrUpdateReleasable(key: K)(value: => F[Releasable[F, V]]) = {

          def valueMetered(ref: Ref[F, Boolean]) = {
            for {
              _        <- ref.set(false)
              start    <- MeasureDuration[F].start
              value    <- value.attempt
              duration <- start
              _        <- metrics.load(duration, value.isRight)
              value    <- value.liftTo[F]
            } yield {
              val release = releaseMetered(start, value.release)
              value.copy(release = release)
            }
          }

          for {
            ref   <- Ref[F].of(true)
            value <- cache.getOrUpdateReleasable(key)(valueMetered(ref))
            hit   <- ref.get
            _     <- metrics.get(hit)
          } yield value
        }

        def getOrUpdateReleasableOpt(key: K)(value: => F[Option[Releasable[F, V]]]) = {

          def valueMetered(ref: Ref[F, Boolean]) = {
            for {
              _        <- ref.set(false)
              start    <- MeasureDuration[F].start
              value    <- value.attempt
              duration <- start
              _        <- metrics.load(duration, value.isRight)
              value    <- value.liftTo[F]
            } yield for {
              value    <- value
            } yield {
              val release = releaseMetered(start, value.release)
              value.copy(release = release)
            }
          }

          for {
            ref   <- Ref[F].of(true)
            value <- cache.getOrUpdateReleasableOpt(key)(valueMetered(ref))
            hit   <- ref.get
            _     <- metrics.get(hit)
          } yield value
        }

        def put(key: K, value: V) = {
          put(key, value, unit)
        }

        def put(key: K, value: V, release: F[Unit]) = {
          for {
            duration <- MeasureDuration[F].start
            _        <- metrics.put
            value    <- cache.put(key, value, releaseMetered(duration, release))
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