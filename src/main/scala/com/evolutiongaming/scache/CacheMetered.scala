package com.evolutiongaming.scache

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Resource, Timer}
import cats.syntax.all._
import com.evolutiongaming.catshelper.Schedule
import com.evolutiongaming.smetrics.MeasureDuration

import scala.concurrent.duration._

object CacheMetered {

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

    for {
      _ <- Schedule(interval, interval)(measureSize)
    } yield {

      def releaseOf(duration: F[FiniteDuration], release: F[Unit]) = {
        for {
          d <- duration
          _ <- metrics.life(d)
          _ <- release
        } yield {}
      }

      new Cache[F, K, V] {

        def get(key: K) = {
          for {
            value <- cache.get(key)
            _     <- metrics.get(value.isDefined)
          } yield value
        }

        def getOrUpdate(key: K)(value: => F[V]) = {
          getOrUpdateReleasable(key) { 
            for {
              value <- value
            } yield {
              Releasable(value, ().pure[F])
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
              val release = releaseOf(start, value.release)
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

        def put(key: K, value: V) = {
          put(key, value, ().pure[F])
        }

        def put(key: K, value: V, release: F[Unit]) = {
          for {
            duration <- MeasureDuration[F].start
            _        <- metrics.put
            value    <- cache.put(key, value, releaseOf(duration, release))
          } yield value
        }

        def size = cache.size

        def keys = cache.keys

        def values = cache.values

        def remove(key: K) = cache.remove(key)

        def clear = cache.clear
      }
    }
  }
}