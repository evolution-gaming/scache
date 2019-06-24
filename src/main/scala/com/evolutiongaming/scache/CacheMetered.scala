package com.evolutiongaming.scache

import cats.effect.concurrent.Ref
import cats.effect.{Clock, Concurrent, Resource, Timer}
import cats.implicits._
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.Schedule

import scala.concurrent.duration._

object CacheMetered {

  def apply[F[_] : Concurrent : Timer, K, V](
    cache: Cache[F, K, V],
    metrics: Cache.Metrics[F]
  ): Resource[F, Cache[F, K, V]] = {

    def measureSize = {
      for {
        size <- cache.size
        _    <- metrics.size(size)
      } yield {}
    }

    for {
      _ <- Schedule(1.minute, 1.minute)(measureSize)
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
              _     <- ref.set(true)
              start <- Clock[F].millis
              value <- value.attempt
              end   <- Clock[F].millis
              time   = end - start
              _     <- metrics.load(time, value.isRight)
              value <- value.raiseOrPure[F]
            } yield value
          }

          for {
            ref   <- Ref[F].of(false)
            value <- cache.getOrUpdate(key)(valueOf(ref))
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