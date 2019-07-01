package com.evolutiongaming.scache

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Resource, Timer}
import cats.implicits._
import com.evolutiongaming.catshelper.Schedule
import com.evolutiongaming.smetrics.MeasureDuration

import scala.concurrent.duration._

object CacheMetered {

  def apply[F[_] : Concurrent : Timer : MeasureDuration, K, V](
    cache: Cache[F, K, V],
    metrics: Cache.Metrics[F],
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

      new Cache[F, K, V] {

        def get(key: K) = {
          for {
            value <- cache.get(key)
            _     <- metrics.get(value.isDefined)
          } yield value
        }

        def getOrUpdate(key: K)(value: => F[V]) = {

          def valueOf(ref: Ref[F, Boolean]) = {
            for {
              _        <- ref.set(false)
              duration <- MeasureDuration[F].start
              value    <- value.attempt
              duration <- duration
              _        <- metrics.load(duration, value.isRight)
              value    <- value.raiseOrPure[F]
            } yield value
          }

          for {
            ref   <- Ref[F].of(true)
            value <- cache.getOrUpdate(key)(valueOf(ref))
            hit   <- ref.get
            _     <- metrics.get(hit)
          } yield value
        }

        def put(key: K, value: V) = cache.put(key, value)

        def size = cache.size

        def keys = cache.keys

        def values = cache.values

        def remove(key: K) = cache.remove(key)

        def clear = cache.clear
      }
    }
  }
}