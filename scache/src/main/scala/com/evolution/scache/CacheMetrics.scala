package com.evolution.scache

import cats.effect.{Concurrent, Ref, Resource}
import cats.syntax.all.*
import cats.{Applicative, Monad}
import com.evolution.scache.CacheMetrics.Directive
import com.evolutiongaming.smetrics.MetricsHelper.*
import com.evolutiongaming.smetrics.{
  CollectorRegistry,
  Counter,
  Gauge,
  LabelNames,
  LabelValues,
  Quantile,
  Quantiles,
  Summary,
}

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.FiniteDuration

trait CacheMetrics[F[_]] {

  def get(hit: Boolean): F[Unit]

  def load(time: FiniteDuration, success: Boolean): F[Unit]

  def life(time: FiniteDuration): F[Unit]

  def put: F[Unit]

  def modify(entryExisted: Boolean, directive: Directive): F[Unit]

  /**
   * Reports the current number of entries. Several caches may report under one name, hence the
   * reported value must be added to the values of the others rather than overwrite them, and a
   * cache being released must report `0` to withdraw its share.
   */
  def size(size: Int): F[Unit]

  def size(latency: FiniteDuration): F[Unit]

  def values(latency: FiniteDuration): F[Unit]

  def keys(latency: FiniteDuration): F[Unit]

  def clear(latency: FiniteDuration): F[Unit]

  def foldMap(latency: FiniteDuration): F[Unit]
}

object CacheMetrics {

  def empty[F[_]: Applicative]: CacheMetrics[F] = const(().pure[F])

  def const[F[_]](unit: F[Unit]): CacheMetrics[F] = new CacheMetrics[F] {

    def get(hit: Boolean) = unit

    def load(time: FiniteDuration, success: Boolean) = unit

    def life(time: FiniteDuration) = unit

    val put = unit

    def modify(entryExisted: Boolean, directive: Directive): F[Unit] = unit

    def size(size: Int) = unit

    def size(latency: FiniteDuration) = unit

    def values(latency: FiniteDuration) = unit

    def keys(latency: FiniteDuration) = unit

    def clear(latency: FiniteDuration) = unit

    def foldMap(latency: FiniteDuration) = unit
  }

  sealed trait Directive {
    override def toString: Prefix = this match {
      case Directive.Put => "put"
      case Directive.Ignore => "ignore"
      case Directive.Remove => "remove"
    }
  }
  object Directive {
    case object Put extends Directive
    case object Ignore extends Directive
    case object Remove extends Directive
  }

  type Name = String

  type Prefix = String

  object Prefix {
    val Default: Prefix = "cache"
  }

  /**
   * Metrics instance per cache name, sharing the collectors. Prefer [[make]]: it tracks the
   * reported size in a `Ref` and withdraws it when the instance is released, this one has to resort
   * to an `AtomicInteger` and relies on the caller reporting `0` on release.
   */
  @deprecated("use make", "6.1.0")
  def of[F[_]: Monad](
    collectorRegistry: CollectorRegistry[F],
    prefix: Prefix = Prefix.Default,
  ): Resource[F, Name => CacheMetrics[F]] = {
    collectors(collectorRegistry, prefix).map { collectors => (name: Name) =>
      val sizeGauge = collectors.sizeGauge.labels(name)
      val sizeReported = new AtomicInteger(0)
      instance(collectors, name) { size =>
        ().pure[F].flatMap { _ =>
          val delta = size - sizeReported.getAndSet(size)
          sizeGauge.inc(delta.toDouble).whenA(delta != 0)
        }
      }
    }
  }

  /**
   * Metrics instance per cache name, sharing the collectors. Several caches may report under one
   * name, each instance adds its size to the shared gauge and takes it back when released.
   */
  def make[F[_]: Concurrent](
    collectorRegistry: CollectorRegistry[F],
    prefix: Prefix = Prefix.Default,
  ): Resource[F, Name => Resource[F, CacheMetrics[F]]] = {
    collectors(collectorRegistry, prefix).map { collectors => (name: Name) =>
      val sizeGauge = collectors.sizeGauge.labels(name)
      Resource.make {
        Ref[F].of(0).map { sizeReported =>
          instance(collectors, name) { size =>
            sizeReported
              .getAndSet(size)
              .flatMap { reported => sizeGauge.inc((size - reported).toDouble).whenA(size != reported) }
          }
        }
      } { metrics =>
        metrics.size(0)
      }
    }
  }

  private final case class Collectors[F[_]](
    gets: LabelValues.`2`[Counter[F]],
    puts: LabelValues.`1`[Counter[F]],
    modifies: LabelValues.`3`[Counter[F]],
    loadResults: LabelValues.`2`[Counter[F]],
    loadTimes: LabelValues.`2`[Summary[F]],
    lifeTimes: LabelValues.`1`[Summary[F]],
    sizeGauge: LabelValues.`1`[Gauge[F]],
    calls: LabelValues.`2`[Summary[F]],
  )

  private def collectors[F[_]](
    collectorRegistry: CollectorRegistry[F],
    prefix: Prefix,
  ): Resource[F, Collectors[F]] = {

    val quantiles = Quantiles(
      Quantile(value = 0.9, error = 0.05),
      Quantile(value = 0.99, error = 0.005),
    )

    for {
      gets <- collectorRegistry.counter(
        name = s"${ prefix }_get",
        help = "Get type: hit or miss",
        labels = LabelNames("name", "type"),
      )
      puts <- collectorRegistry.counter(
        name = s"${ prefix }_put",
        help = "Put",
        labels = LabelNames("name"),
      )
      modifies <- collectorRegistry.counter(
        name = s"${ prefix }_modify",
        help = "Modify, labeled by modification input (entry was present or not), and output (put, keep, or remove)",
        labels = LabelNames("name", "existing_entry", "result"),
      )
      loadResults <- collectorRegistry.counter(
        name = s"${ prefix }_load_result",
        help = "Load result: success or failure",
        labels = LabelNames("name", "result"),
      )
      loadTimes <- collectorRegistry.summary(
        name = s"${ prefix }_load_time",
        help = s"Load time in seconds",
        quantiles = quantiles,
        labels = LabelNames("name", "result"),
      )
      lifeTimes <- collectorRegistry.summary(
        name = s"${ prefix }_life_time",
        help = s"Life time in seconds",
        quantiles = quantiles,
        labels = LabelNames("name"),
      )
      sizeGauge <- collectorRegistry.gauge(
        name = s"${ prefix }_size",
        help = s"Cache size",
        labels = LabelNames("name"),
      )
      calls <- collectorRegistry.summary(
        name = s"${ prefix }_call_latency",
        help = "Call latency in seconds",
        quantiles = quantiles,
        labels = LabelNames("name", "type"),
      )
    } yield {
      Collectors(gets, puts, modifies, loadResults, loadTimes, lifeTimes, sizeGauge, calls)
    }
  }

  private def instance[F[_]: Monad](
    collectors: Collectors[F],
    name: Name,
  )(
    reportSize: Int => F[Unit],
  ): CacheMetrics[F] = {

    val hitCounter = collectors.gets.labels(name, "hit")

    val missCounter = collectors.gets.labels(name, "miss")

    val successCounter = collectors.loadResults.labels(name, "success")

    val failureCounter = collectors.loadResults.labels(name, "failure")

    val successSummary = collectors.loadTimes.labels(name, "success")

    val failureSummary = collectors.loadTimes.labels(name, "failure")

    val putCounter = collectors.puts.labels(name)

    val lifeTimeSummary = collectors.lifeTimes.labels(name)

    val sizeSummary = collectors.calls.labels(name, "size")

    val keysSummary = collectors.calls.labels(name, "keys")

    val valuesSummary = collectors.calls.labels(name, "values")

    val clearSummary = collectors.calls.labels(name, "clear")

    val foldMapSummary = collectors.calls.labels(name, "foldMap")

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
        lifeTimeSummary.observe(time.toNanos.nanosToSeconds)
      }

      val put = putCounter.inc()

      def modify(entryExisted: Boolean, directive: Directive): F[Unit] = {
        collectors.modifies.labels(name, entryExisted.toString, directive.toString).inc()
      }

      def size(size: Int) = reportSize(size)

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
