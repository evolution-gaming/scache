package com.evolutiongaming.scache

import cats.{Applicative, Monad}
import cats.effect.Resource
import cats.implicits._
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics.{CollectorRegistry, LabelNames, Quantile, Quantiles}

import scala.concurrent.duration.FiniteDuration

trait CacheMetrics[F[_]] {

  def get(hit: Boolean): F[Unit]

  def load(time: FiniteDuration, success: Boolean): F[Unit]

  def size(size: Int): F[Unit]
}

object CacheMetrics {

  def empty[F[_] : Applicative]: CacheMetrics[F] = const(().pure[F])


  def const[F[_]](unit: F[Unit]): CacheMetrics[F] = new CacheMetrics[F] {

    def get(hit: Boolean) = unit

    def load(time: FiniteDuration, success: Boolean) = unit

    def size(size: Int) = unit
  }


  type Name = String

  type Prefix = String

  object Prefix {
    val Default: Prefix = "cache"
  }


  def of[F[_] : Monad](
    collectorRegistry: CollectorRegistry[F],
    prefix: Prefix = Prefix.Default,
  ): Resource[F, Name => CacheMetrics[F]] = {

    val getCounter = collectorRegistry.counter(
      name = s"${ prefix }_get",
      help = "Get type: hit or miss",
      labels = LabelNames("name", "type"))

    val loadResultCounter = collectorRegistry.counter(
      name = s"${ prefix }_load_result",
      help = "Load result: success or failure",
      labels = LabelNames("name", "result"))

    val loadTimeSummary = collectorRegistry.summary(
      name = s"${ prefix }_load_time",
      help = s"Load time in seconds",
      quantiles = Quantiles(
        Quantile(value = 0.9, error = 0.05),
        Quantile(value = 0.99, error = 0.005)),
      labels = LabelNames("name", "result"))

    val sizeGauge = collectorRegistry.gauge(
      name = s"${ prefix }_size",
      help = s"Cache size",
      labels = LabelNames("name"))

    for {
      getsCounter       <- getCounter
      loadResultCounter <- loadResultCounter
      loadTimeSummary   <- loadTimeSummary
      sizeGauge         <- sizeGauge
    } yield {
      name: Name =>

        val hitCounter = getsCounter.labels(name, "hit")

        val missCounter = getsCounter.labels(name, "miss")

        val successCounter = loadResultCounter.labels(name, "success")

        val failureCounter = loadResultCounter.labels(name, "failure")

        val successSummary = loadTimeSummary.labels(name, "success")

        val failureSummary = loadTimeSummary.labels(name, "failure")

        new CacheMetrics[F] {

          def get(hit: Boolean) = {
            val counter = if (hit) hitCounter else missCounter
            counter.inc()
          }

          def size(size: Int) = {
            sizeGauge.labels(name).set(size.toDouble)
          }

          def load(time: FiniteDuration, success: Boolean) = {
            val resultCounter = if (success) successCounter else failureCounter
            val timeSummary = if (success) successSummary else failureSummary
            for {
              _ <- resultCounter.inc()
              _ <- timeSummary.observe(time.toNanos.nanosToSeconds)
            } yield {}
          }
        }
    }
  }
}