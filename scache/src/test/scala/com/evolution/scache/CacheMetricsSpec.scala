package com.evolution.scache

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.evolution.scache.IOSuite.*
import com.evolutiongaming.smetrics.{CollectorRegistry, Counter, Gauge, Histogram, Info, Summary}
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.annotation.nowarn

class CacheMetricsSpec extends AsyncFunSuite with Matchers {

  private def registry(gauge: Ref[IO, Double]): CollectorRegistry[IO] = {
    val gauge1 = new Gauge[IO] {
      def inc(value: Double): IO[Unit] = gauge.update(_ + value)
      def dec(value: Double): IO[Unit] = gauge.update(_ - value)
      def set(value: Double): IO[Unit] = gauge.set(value)
    }
    CollectorRegistry.const[IO](
      gauge1.pure[IO],
      Counter.empty[IO].pure[IO],
      Summary.empty[IO].pure[IO],
      Histogram.empty[IO].pure[IO],
      Info.empty[IO].pure[IO],
    )
  }

  private def expect(gauge: Ref[IO, Double], value: Double): IO[Unit] =
    gauge.get.map { _ shouldEqual value }.void

  test("of: size sums over caches reporting under the same name") {
    val result = for {
      gauge <- Ref.of[IO, Double](0)
      _ <- (CacheMetrics.of(registry(gauge)): @nowarn("cat=deprecation")).use { metricsOf =>
        val a = metricsOf("name")
        val b = metricsOf("name")
        for {
          _ <- a.size(10)
          _ <- b.size(5)
          _ <- expect(gauge, 15)
          _ <- a.size(7)
          _ <- expect(gauge, 12)
          _ <- a.size(7)
          _ <- expect(gauge, 12)
          _ <- b.size(0)
          _ <- expect(gauge, 7)
          _ <- a.size(0)
          _ <- expect(gauge, 0)
        } yield {}
      }
    } yield {}
    result.run()
  }

  test("make: size sums over caches reporting under the same name and is withdrawn on release") {
    val result = for {
      gauge <- Ref.of[IO, Double](0)
      _ <- CacheMetrics.make(registry(gauge)).use { metricsOf =>
        metricsOf("name").use { a =>
          for {
            _ <- a.size(10)
            _ <- metricsOf("name").use { b =>
              for {
                _ <- b.size(5)
                _ <- expect(gauge, 15)
                _ <- a.size(7)
                _ <- expect(gauge, 12)
                _ <- a.size(7)
                _ <- expect(gauge, 12)
              } yield {}
            }
            _ <- expect(gauge, 7)
          } yield {}
        }
      }
      _ <- expect(gauge, 0)
    } yield {}
    result.run()
  }
}
