package com.evolutiongaming.scache

import cats.{Applicative, Monad}
import cats.effect.Resource
import cats.syntax.all._
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics.{CollectorRegistry, LabelNames, Quantile, Quantiles}

import scala.concurrent.duration.FiniteDuration

trait CacheMetrics[F[_]] {

  def get(hit: Boolean): F[Unit]

  def load(time: FiniteDuration, success: Boolean): F[Unit]

  def life(time: FiniteDuration): F[Unit]

  def put: F[Unit]

  def size(size: Int): F[Unit]

  def size(latency: FiniteDuration): F[Unit]

  def values(latency: FiniteDuration): F[Unit]

  def keys(latency: FiniteDuration): F[Unit]

  def clear(latency: FiniteDuration): F[Unit]

  def foldMap(latency: FiniteDuration): F[Unit]
}

object CacheMetrics {

  def empty[F[_] : Applicative]: CacheMetrics[F] = const(().pure[F])


  def const[F[_]](unit: F[Unit]): CacheMetrics[F] = new CacheMetrics[F] {

    def get(hit: Boolean) = unit

    def load(time: FiniteDuration, success: Boolean) = unit

    def life(time: FiniteDuration) = unit

    val put = unit

    def size(size: Int) = unit

    def size(latency: FiniteDuration) = unit

    def values(latency: FiniteDuration) = unit

    def keys(latency: FiniteDuration) = unit

    def clear(latency: FiniteDuration) = unit

    def foldMap(latency: FiniteDuration) = unit
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

    val putCounter = collectorRegistry.counter(
      name = s"${ prefix }_put",
      help = "Put",
      labels = LabelNames("name"))

    val loadResultCounter = collectorRegistry.counter(
      name = s"${ prefix }_load_result",
      help = "Load result: success or failure",
      labels = LabelNames("name", "result"))

    val quantiles = Quantiles(
      Quantile(value = 0.9, error = 0.05),
      Quantile(value = 0.99, error = 0.005))

    val loadTimeSummary = collectorRegistry.summary(
      name = s"${ prefix }_load_time",
      help = s"Load time in seconds",
      quantiles = quantiles,
      labels = LabelNames("name", "result"))

    val sizeGauge = collectorRegistry.gauge(
      name = s"${ prefix }_size",
      help = s"Cache size",
      labels = LabelNames("name"))

    val lifeTimeSummary = collectorRegistry.summary(
      name = s"${ prefix }_life_time",
      help = s"Life time in seconds",
      quantiles = quantiles,
      labels = LabelNames("name"))

    val callSummary = collectorRegistry.summary(
      name = s"${ prefix }_call_latency",
      help = "Call latency in seconds",
      quantiles = quantiles,
      labels = LabelNames("name", "type"))

    for {
      getsCounter       <- getCounter
      putCounter        <- putCounter
      loadResultCounter <- loadResultCounter
      loadTimeSummary   <- loadTimeSummary
      lifeTimeSummary   <- lifeTimeSummary
      sizeGauge         <- sizeGauge
      callSummary       <- callSummary
    } yield {
      name: Name =>

        val hitCounter = getsCounter.labels(name, "hit")

        val missCounter = getsCounter.labels(name, "miss")

        val successCounter = loadResultCounter.labels(name, "success")

        val failureCounter = loadResultCounter.labels(name, "failure")

        val successSummary = loadTimeSummary.labels(name, "success")

        val failureSummary = loadTimeSummary.labels(name, "failure")

        val putCounter1 = putCounter.labels(name)

        val lifeTimeSummary1 = lifeTimeSummary.labels(name)

        val sizeSummary = callSummary.labels(name, "size")

        val keysSummary = callSummary.labels(name, "keys")

        val valuesSummary = callSummary.labels(name, "values")

        val clearSummary = callSummary.labels(name, "clear")

        val foldMapSummary = callSummary.labels(name, "foldMap")

        new CacheMetrics[F] {

          def get(hit: Boolean) = {
            val counter = if (hit) hitCounter else missCounter
            counter.inc()
          }                               

          def load(time: FiniteDuration, success: Boolean) = {
            val resultCounter = if (success) successCounter else failureCounter
            val timeSummary = if (success) successSummary else failureSummary
            for {
              _ <- resultCounter.inc()
              _ <- timeSummary.observe(time.toNanos.nanosToSeconds)
            } yield {}
          }

          def life(time: FiniteDuration) = {
            lifeTimeSummary1.observe(time.toNanos.nanosToSeconds)
          }

          val put = putCounter1.inc()

          def size(size: Int) = {
            sizeGauge.labels(name).set(size.toDouble)
          }

          def size(latency: FiniteDuration) = {
            sizeSummary.observe(latency.toNanos.nanosToSeconds)
          }

          def values(latency: FiniteDuration) = {
            valuesSummary.observe(latency.toNanos.nanosToSeconds)
          }

          def keys(latency: FiniteDuration) = {
            keysSummary.observe(latency.toNanos.nanosToSeconds)
          }

          def clear(latency: FiniteDuration) = {
            clearSummary.observe(latency.toNanos.nanosToSeconds)
          }

          def foldMap(latency: FiniteDuration) = {
            foldMapSummary.observe(latency.toNanos.nanosToSeconds)
          }
        }
    }
  }
}