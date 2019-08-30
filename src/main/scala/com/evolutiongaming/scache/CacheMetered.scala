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
    metrics: CacheMetrics[F],
    interval: FiniteDuration = 1.minute
  ): Resource[F, Cache[F, K, V]] = {

    def measureSize = {
      for {
        size <- cache.size
        _    <- metrics.size(size)
      } yield {}
    }

    def measureLoad[A, B](value: => F[A])(f: (() => F[A]) => F[B]): F[B] = {

      def measured(ref: Ref[F, Boolean]) = {
        for {
          _        <- ref.set(false)
          duration <- MeasureDuration[F].start
          value    <- value.attempt
          duration <- duration
          _        <- metrics.load(duration, value.isRight)
          value    <- value.liftTo[F]
        } yield value
      }

      for {
        ref   <- Ref[F].of(true)
        value <- f(() => measured(ref))
        hit   <- ref.get
        _     <- metrics.get(hit)
      } yield value
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
          measureLoad(value) { value => cache.getOrUpdate(key)(value()) }
        }

        def getOrUpdateReleasable(key: K)(value: => F[Releasable[F, V]]) = {
          measureLoad(value) { value => cache.getOrUpdateReleasable(key)(value()) }
        }

        def put(key: K, value: V) = {
          for {
            _ <- metrics.put
            v <- cache.put(key, value)
          } yield v
        }

        def put(key: K, value: V, release: F[Unit]) = {
          for {
            _ <- metrics.put
            v <- cache.put(key, value, release)
          } yield v
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