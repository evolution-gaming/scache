package com.evolution.scache.v1

import cats.effect.syntax.all.*
import cats.effect.{Concurrent, Resource, Temporal}
import cats.syntax.all.*
import cats.{Hash, Parallel}
import com.evolution.scache.{Cache, NrOfPartitions, Partitions}
import com.evolutiongaming.catshelper.CatsHelper.*
import com.evolutiongaming.catshelper.Runtime

/**
 * Constructors of the frozen pre-`MapRef` implementation, mirroring `Cache.loading` and
 * `Cache.expiring` as they were before the rewrite, so that the benchmarks can build the old and
 * the new cache the same way.
 */
object CacheV1 {

  def loading[F[_]: Concurrent: Parallel: Runtime, K, V](
    partitions: Option[Int] = None,
  ): Resource[F, Cache[F, K, V]] = {

    implicit val hash: Hash[K] = Hash.fromUniversalHashCode[K]

    val result = for {
      nrOfPartitions <- partitions
        .map { _.pure[F] }
        .getOrElse { NrOfPartitions[F]() }
        .toResource
      cache = LoadingCache.of(LoadingCache.EntryRefs.empty[F, K, V])
      partitions <- Partitions.of[Resource[F, _], K, Cache[F, K, V]](nrOfPartitions, _ => cache)
    } yield {
      Cache.fromPartitions(partitions)
    }
    result.breakFlatMapChain
  }

  def expiring[F[_]: Temporal: Runtime: Parallel, K, V](
    config: ExpiringCache.Config[F, K, V],
    partitions: Option[Int] = None,
  ): Resource[F, Cache[F, K, V]] = {

    implicit val hash: Hash[K] = Hash.fromUniversalHashCode[K]

    val result = for {
      nrOfPartitions <- partitions
        .map { _.pure[F] }
        .getOrElse { NrOfPartitions[F]() }
        .toResource
      config1 = config
        .maxSize
        .fold {
          config
        } { maxSize =>
          config.copy(maxSize = (maxSize * 1.1 / nrOfPartitions).toInt.some)
        }
      cache = ExpiringCache.of[F, K, V](config1)
      partitions <- Partitions.of[Resource[F, _], K, Cache[F, K, V]](nrOfPartitions, _ => cache)
    } yield {
      Cache.fromPartitions(partitions)
    }

    result.breakFlatMapChain
  }
}
