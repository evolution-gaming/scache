package com.evolution.scache

import cats.effect.{IO, IOApp, Resource}
import cats.syntax.all.*

import scala.concurrent.duration.*

/**
 * Load test comparing cache flavors under contention. Not executed as part of the test suite, run
 * it with:
 * {{{
 * sbt "scache/Test/runMain com.evolution.scache.CacheLoadTest"
 * }}}
 */
object CacheLoadTest extends IOApp.Simple {

  private val fibers = Runtime.getRuntime.availableProcessors
  private val opsPerFiber = 100000
  private val keySpace = 10000

  val run: IO[Unit] = {
    val caches = List(
      ("LoadingCache (single partition)", LoadingCache.of[IO, Int, Int]),
      ("Cache.loading (partitioned)", Cache.loading[IO, Int, Int]),
      (
        "Cache.expiring (partitioned)",
        Cache.expiring[IO, Int, Int](ExpiringCache.Config[IO, Int, Int](expireAfterRead = 1.minute)),
      ),
    )
    for {
      _ <- IO.println(f"fibers=$fibers, ops/fiber=$opsPerFiber, keySpace=$keySpace")
      _ <- caches.traverse_ { case (name, cache) =>
        IO.println(s"--- $name") *> scenarios(cache)
      }
    } yield {}
  }

  private def scenarios(cache: Resource[IO, Cache[IO, Int, Int]]): IO[Unit] = {
    cache.use { cache =>
      for {
        _ <- measure("getOrUpdate, insert distinct keys") {
          parRun { (fiber, i) => cache.getOrUpdate(fiber * opsPerFiber + i)(i.pure[IO]).void }
        }
        _ <- cache.clear.flatten
        _ <- (0 until keySpace).toList.traverse_ { key => cache.put(key, key).flatten }
        _ <- measure("getOrUpdate, hit random keys") {
          parRun { (fiber, i) =>
            val key = scramble(fiber * opsPerFiber + i) % keySpace
            cache.getOrUpdate(key)(key.pure[IO]).void
          }
        }
        _ <- measure("getOrUpdate, hit single hot key") {
          parRun { (_, _) => cache.getOrUpdate(0)(0.pure[IO]).void }
        }
        _ <- measure("put, replace random keys") {
          parRun { (fiber, i) =>
            val key = scramble(fiber * opsPerFiber + i) % keySpace
            cache.put(key, i).flatten.void
          }
        }
        _ <- measure("mixed get/put/remove, random keys") {
          parRun { (fiber, i) =>
            val n = scramble(fiber * opsPerFiber + i)
            val key = n % keySpace
            (n / keySpace) % 10 match {
              case 0 => cache.put(key, i).flatten.void
              case 1 => cache.remove(key).flatten.void
              case _ => cache.getOrUpdate(key)(i.pure[IO]).void
            }
          }
        }
      } yield {}
    }
  }

  private def parRun(op: (Int, Int) => IO[Unit]): IO[Unit] = {
    (0 until fibers)
      .toList
      .parTraverse_ { fiber =>
        (0 until opsPerFiber).toList.traverse_ { i => op(fiber, i) }
      }
  }

  private def measure(name: String)(io: IO[Unit]): IO[Unit] = {
    for {
      start <- IO.monotonic
      _ <- io
      end <- IO.monotonic
      millis = (end - start).toMillis.max(1)
      opsPerSec = fibers.toLong * opsPerFiber * 1000 / millis
      _ <- IO.println(f"$name%-42s ${ millis }%6d ms  $opsPerSec%,12d ops/s")
    } yield {}
  }

  private def scramble(i: Int): Int = {
    val h = i * 0x9e3775cd
    (h ^ (h >>> 16)) & Int.MaxValue
  }
}
