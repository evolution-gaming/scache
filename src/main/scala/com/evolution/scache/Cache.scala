package com.evolution.scache

import cats.effect.kernel.MonadCancel
import cats.effect.{Concurrent, Resource, Temporal}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.{Functor, Hash, Monad, MonadThrow, Monoid, Parallel, ~>}
import cats.kernel.CommutativeMonoid
import com.evolutiongaming.catshelper.CatsHelper.*
import com.evolutiongaming.catshelper.Runtime
import com.evolutiongaming.smetrics.MeasureDuration

import scala.annotation.nowarn
import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

trait Cache[F[_], K, V] {

  type Release = F[Unit]

  type Released = F[Unit]

  def get(key: K): F[Option[V]]

  def get1(key: K): F[Option[Either[F[V], V]]]

  @deprecated("use `getOrElse1` instead", "4.3.0")
  def getOrElse(key: K, default: => F[V]): F[V]

  /**
    * Does not run `value` concurrently for the same key
    */
  def getOrUpdate(key: K)(value: => F[V]): F[V]

  /**
    * Does not run `value` concurrently for the same key
    * release will be called upon key removal from the cache
    *
    * @return either A passed as argument or `Either[F[V], V]` that represents loading or loaded value
    */
  def getOrUpdate1[A](key: K)(value: => F[(A, V, Option[Release])]): F[Either[A, Either[F[V], V]]]

  /**
    * Does not run `value` concurrently for the same key
    * In case of none returned, value will be ignored by cache
    */
  def getOrUpdateOpt(key: K)(value: => F[Option[V]]): F[Option[V]]

  /**
    * Does not run `value` concurrently for the same key
    * Releasable.release will be called upon key removal from the cache
    */
  @deprecated("use `getOrUpdateResource` instead", "4.3.0")
  def getOrUpdateReleasable(key: K)(value: => F[Releasable[F, V]]): F[V]

  /**
    * Does not run `value` concurrently for the same key
    * Releasable.release will be called upon key removal from the cache
    * In case of none returned, value will be ignored by cache
    */
  @deprecated("use `getOrUpdateResourceOpt` instead", "4.3.0")
  def getOrUpdateReleasableOpt(key: K)(value: => F[Option[Releasable[F, V]]]): F[Option[V]]

  /**
    * @return previous value if any, possibly not yet loaded
    */
  def put(key: K, value: V): F[F[Option[V]]]

  /**
    * @return previous value if any, possibly not yet loaded
    */
  def put(key: K, value: V, release: Release): F[F[Option[V]]]

  /**
    * @return previous value if any, possibly not yet loaded
    */
  def put(key: K, value: V, release: Option[Release]): F[F[Option[V]]]


  def contains(key: K): F[Boolean]
  

  def size: F[Int]


  def keys: F[Set[K]]

  /**
    * Might be an expensive call
    */
  def values: F[Map[K, F[V]]]

  /**
    * @return either map of either loading or loaded value
    * Might be an expensive call
    */
  def values1: F[Map[K, Either[F[V], V]]]

  /**
    * @return previous value if any, possibly not yet loaded
    */
  def remove(key: K): F[F[Option[V]]]


  /**
    * Removes loading values from the cache, however does not cancel them
    */
  def clear: F[Released]

  def foldMap[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]): F[A]

  def foldMapPar[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]): F[A]
}

object Cache {

  def empty[F[_]: Monad, K, V]: Cache[F, K, V] = {
    abstract class Empty extends Cache.Abstract0[F, K, V]

    new Empty {

      def get(key: K) = none[V].pure[F]

      def get1(key: K) = none[Either[F[V], V]].pure[F]

      def getOrUpdate(key: K)(value: => F[V]) = value

      def getOrUpdate1[A](key: K)(value: => F[(A, V, Option[Release])]) = {
        value.map { case (a, _, _) => a.asLeft[Either[F[V], V]] }
      }

      def getOrUpdateOpt(key: K)(value: => F[Option[V]]) = value

      @nowarn("msg=deprecated")
      def getOrUpdateReleasableOpt(key: K)(value: => F[Option[Releasable[F, V]]]) = {
        value.map { _.map { _.value } }
      }

      def put(key: K, value: V, release: Option[F[Unit]]) = none[V].pure[F].pure[F]

      def contains(key: K) = false.pure[F]

      def size = 0.pure[F]

      def keys = Set.empty[K].pure[F]

      def values = Map.empty[K, F[V]].pure[F]

      def values1 = Map.empty[K, Either[F[V], V]].pure[F]

      def remove(key: K) = none[V].pure[F].pure[F]

      def clear = ().pure[F].pure[F]

      def foldMap[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = Monoid[A].empty.pure[F]

      def foldMapPar[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]) = Monoid[A].empty.pure[F]
    }
  }

  @deprecated("use `loading1` instead", "4.1.1")
  def loading[F[_]: Concurrent: Runtime, K, V]: Resource[F, Cache[F, K, V]] = {
    implicit val parallel: Parallel[F] = Parallel.identity
    loading1(none)
  }

  @deprecated("use `loading1` instead", "4.1.1")
  def loading[F[_]: Concurrent: Runtime, K, V](partitions: Int): Resource[F, Cache[F, K, V]] = {
    implicit val parallel: Parallel[F] = Parallel.identity
    loading1(partitions.some)
  }

  @deprecated("use `loading1` instead", "4.1.1")
  def loading[F[_]: Concurrent: Runtime, K, V](partitions: Option[Int] = None): Resource[F, Cache[F, K, V]] = {
    implicit val parallel: Parallel[F] = Parallel.identity
    loading1[F, K, V](partitions)
  }

  def loading1[F[_]: Concurrent: Parallel: Runtime, K, V]: Resource[F, Cache[F, K, V]] = {
    loading1(none)
  }

  def loading1[F[_]: Concurrent: Parallel: Runtime, K, V](partitions: Int): Resource[F, Cache[F, K, V]] = {
    loading1(partitions.some)
  }

  def loading1[F[_]: Concurrent: Parallel: Runtime, K, V](partitions: Option[Int] = None): Resource[F, Cache[F, K, V]] = {

    implicit val hash: Hash[K] = Hash.fromUniversalHashCode[K]

    val result = for {
      nrOfPartitions <- partitions
        .map { _.pure[F] }
        .getOrElse { NrOfPartitions[F]() }
        .toResource
      cache           = LoadingCache.of(LoadingCache.EntryRefs.empty[F, K, V])
      partitions     <- Partitions.of[Resource[F, _], K, Cache[F, K, V]](nrOfPartitions, _ => cache)
    } yield {
      fromPartitions1(partitions)
    }
    result.breakFlatMapChain
  }


  @deprecated("use `expiring` with `config` argument", "2.3.0")
  def expiring[F[_]: Concurrent: Temporal: Runtime: Parallel, K, V](
    expireAfter: FiniteDuration,
  ): Resource[F, Cache[F, K, V]] = {
    expiring(
      ExpiringCache.Config(expireAfterRead = expireAfter),
      none)
  }

  @deprecated("use `expiring` with `config` argument", "2.3.0")
  def expiring[F[_]: Concurrent: Temporal: Runtime: Parallel, K, V](
    expireAfter: FiniteDuration,
    maxSize: Option[Int],
    refresh: Option[ExpiringCache.Refresh[K, F[V]]]
  ): Resource[F, Cache[F, K, V]] = {
    expiring(
      ExpiringCache.Config(
        expireAfterRead = expireAfter,
        maxSize = maxSize,
        refresh = refresh.map { refresh => refresh.copy(value = (k: K) => refresh.value(k).map { _.some }) }),
      none)
  }


  @deprecated("use `expiring` with `config` argument", "2.3.0")
  def expiring[F[_]: Concurrent: Temporal: Runtime: Parallel, K, V](
    expireAfter: FiniteDuration,
    maxSize: Option[Int],
    refresh: Option[ExpiringCache.Refresh[K, F[V]]],
    partitions: Option[Int]
  ): Resource[F, Cache[F, K, V]] = {
    expiring(
      ExpiringCache.Config(
        expireAfterRead = expireAfter,
        maxSize = maxSize,
        refresh = refresh.map { refresh => refresh.copy(value = (k: K) => refresh.value(k).map { _.some }) }),
      partitions)
  }


  def expiring[F[_]: Temporal: Runtime: Parallel, K, V](
    config: ExpiringCache.Config[F, K, V],
    partitions: Option[Int] = None
  ): Resource[F, Cache[F, K, V]] = {

    implicit val hash: Hash[K] = Hash.fromUniversalHashCode[K]

    val result = for {
      nrOfPartitions <- partitions
        .map { _.pure[F] }
        .getOrElse { NrOfPartitions[F]() }
        .toResource
      config1         = config
        .maxSize
        .fold {
          config
        } {
          maxSize => config.copy(maxSize = (maxSize * 1.1 / nrOfPartitions).toInt.some)
        }
      cache           = ExpiringCache.of[F, K, V](config1)
      partitions     <- Partitions.of[Resource[F, _], K, Cache[F, K, V]](nrOfPartitions, _ => cache)
    } yield {
      fromPartitions1(partitions)
    }

    result.breakFlatMapChain
  }

  @deprecated("use `fromPartitions1` instead", "4.3.0")
  def fromPartitions[F[_]: Monad: Parallel, K, V](partitions: Partitions[K, Cache[F, K, V]]): Cache[F, K, V] = {
    PartitionedCache.apply1(partitions)
  }

  def fromPartitions1[F[_]: MonadThrow: Parallel, K, V](partitions: Partitions[K, Cache[F, K, V]]): Cache[F, K, V] = {
    PartitionedCache.apply2(partitions)
  }

  private[scache] abstract class Abstract0[F[_]: Monad, K, V] extends Cache[F, K, V] { self =>

    def getOrElse(key: K, default: => F[V]) = {
      self
        .get(key)
        .flatMap {
          case Some(a) => a.pure[F]
          case None    => default
        }
    }

    @nowarn("msg=deprecated")
    def getOrUpdateReleasable(key: K)(value: => F[Releasable[F, V]]) = {
      self
        .getOrUpdate1(key) { value.map { a => (a.value, a.value, a.release.some) } }
        .flatMap {
          case Right(Right(a)) => a.pure[F]
          case Right(Left(a))  => a
          case Left(a)         => a.pure[F]
        }
    }

    def put(key: K, value: V) = self.put(key, value, none)

    def put(key: K, value: V, release: Release) = self.put(key, value, release.some)
  }

  private[scache] abstract class Abstract1[F[_]: MonadThrow, K, V] extends Abstract0[F, K, V] { self =>

    def getOrUpdateOpt(key: K)(value: => F[Option[V]]) = {
      self
        .getOrUpdate1(key) {
          value.flatMap {
            case Some(a) => (a, a, none[Release]).pure[F]
            case None    => NoneError.raiseError[F, (V, V, Option[Release])]
          }
        }
        .flatMap {
          case Right(Right(a)) => a.pure[F]
          case Right(Left(a))  => a
          case Left(a)         => a.pure[F]
        }
        .map { _.some }
        .recover { case NoneError => none }
    }

    @nowarn("msg=deprecated")
    def getOrUpdateReleasableOpt(key: K)(value: => F[Option[Releasable[F, V]]]) = {
      self
        .getOrUpdateOpt1(key) {
          value.map { releasable =>
            releasable.map { releasable =>
              (releasable.value, releasable.value, releasable.release.some)
            }
          }
        }
        .flatMap {
          case Some(Right(Right(a))) => a.some.pure[F]
          case Some(Right(Left(a)))  => a.map { _.some }
          case Some(Left(a))         => a.some.pure[F]
          case None                  => none[V].pure[F]
        }
    }
  }

  private sealed abstract class MapK

  private[scache] case object NoneError extends RuntimeException with NoStackTrace

  implicit class CacheOps[F[_], K, V](val self: Cache[F, K, V]) extends AnyVal {

    def withMetrics(
      metrics: CacheMetrics[F])(implicit
      temporal: Temporal[F],
      measureDuration: MeasureDuration[F]
    ): Resource[F, Cache[F, K, V]] = {
      CacheMetered(self, metrics)
    }

    def mapK[G[_]](fg: F ~> G, gf: G ~> F)(implicit F: Functor[F]): Cache[G, K, V] = {
      new MapK with Cache[G, K, V] {

        def get(key: K) = fg(self.get(key))

        def get1(key: K) = {
          fg {
            self
              .get1(key)
              .map { _.map { _.leftMap { a => fg(a) } } }
          }
        }

        @nowarn("msg=method getOrElse in trait Cache is deprecated")
        def getOrElse(key: K, default: => G[V]) = fg(self.getOrElse(key, gf(default)))

        def getOrUpdate(key: K)(value: => G[V]) = fg(self.getOrUpdate(key)(gf(value)))

        def getOrUpdate1[A](key: K)(value: => G[(A, V, Option[Release])]) = {
          fg {
            self
              .getOrUpdate1(key) { gf(value).map { case (a, value, release) => (a, value, release.map { a => gf(a) }) } }
              .map { _.map { _.leftMap { a => fg(a) } } }
          }
        }

        def getOrUpdateOpt(key: K)(value: => G[Option[V]]) = {
          fg(self.getOrUpdateOpt(key)(gf(value)))
        }

        @nowarn("msg=deprecated")
        def getOrUpdateReleasable(key: K)(value: => G[Releasable[G, V]]) = {
          fg(self.getOrUpdateReleasable(key)(gf(value).map(_.mapK(gf))))
        }

        @nowarn("msg=deprecated")
        def getOrUpdateReleasableOpt(key: K)(value: => G[Option[Releasable[G, V]]]) = {
          fg(self.getOrUpdateReleasableOpt(key)(gf(value).map(_.map(_.mapK(gf)))))
        }

        def put(key: K, value: V) = {
          fg {
            self
              .put(key, value)
              .map { a => fg(a) }
          }
        }

        def put(key: K, value: V, release: Release) = {
          fg {
            self
              .put(key, value, gf(release))
              .map { a => fg(a) }
          }
        }

        def put(key: K, value: V, release: Option[Release]) = {
          fg {
            self
              .put(key, value, release.map { a => gf(a) })
              .map { a => fg(a) }
          }
        }

        def contains(key: K) = fg(self.contains(key))

        def size = fg(self.size)

        def keys = fg(self.keys)

        def values = fg(self.values.map(_.map { case (k, v) => (k, fg(v)) }))

        def values1 = fg {
          self
            .values1
            .map { _.map { case (k, v) => (k, v.leftMap { v => fg(v) }) } }
        }

        def remove(key: K) = fg(self.remove(key).map(fg.apply))

        def clear = fg(self.clear.map { a => fg(a): Released })

        def foldMap[A: CommutativeMonoid](f: (K, Either[G[V], V]) => G[A]) = {
          fg(self.foldMap { case (k, v) => gf(f(k, v.leftMap { v => fg(v) })) })
        }

        def foldMapPar[A: CommutativeMonoid](f: (K, Either[G[V], V]) => G[A]) = {
          fg(self.foldMap { case (k, v) => gf(f(k, v.leftMap { v => fg(v) })) })
        }
      }
    }

    def withFence(implicit F: Concurrent[F]): Resource[F, Cache[F, K, V]] = CacheFenced.of1(self)

    def getOrElse1(key: K, value: => F[V])(implicit F: Monad[F]): F[V] = {
      self
        .get(key)
        .flatMap {
          case Some(a) => a.pure[F]
          case None    => value
        }
    }

    def getOrUpdate2[A](
      key: K)(
      value: => F[(A, V, Option[Cache[F, K, V]#Release])])(implicit
      F: Monad[F]
    ): F[Either[A, V]] = {
      self
        .getOrUpdate1(key) { value }
        .flatMap {
          case Right(Right(a)) => a.asRight[A].pure[F]
          case Right(Left(a))  => a.map { _.asRight[A] }
          case Left(a)         => a.asLeft[V].pure[F]
        }
    }

    /**
      * Does not run `value` concurrently for the same key
      * release will be called upon key removal from the cache
      * In case of none returned, value will be ignored by cache
      */
    def getOrUpdateOpt1[A](key: K)(
      value: => F[Option[(A, V, Option[Cache[F, K, V]#Release])]])(implicit
      F: MonadThrow[F]
    ): F[Option[Either[A, Either[F[V], V]]]] = {
      self
        .getOrUpdate1(key) {
          value.flatMap {
            case Some((a, value, release)) => (a, value, release).pure[F]
            case None                      => NoneError.raiseError[F, (A, V, Option[Cache[F, K, V]#Release])]
          }
        }
        .map { _.some }
        .recover { case NoneError => none }
    }

    /**
      * Does not run `value` concurrently for the same key
      * Resource will be release upon key removal from the cache
      */
    def getOrUpdateResource(key: K)(value: => Resource[F, V])(implicit F: MonadCancel[F, Throwable]): F[V] = {
      self
        .getOrUpdate1(key) {
          value
            .allocated
            .map { case (a, release) => (a, a, release.some) }
        }
        .flatMap {
          case Right(Right(a)) => a.pure[F]
          case Right(Left(a))  => a
          case Left(a)         => a.pure[F]
        }
    }

    /**
      * Does not run `value` concurrently for the same key
      * Resource will be release upon key removal from the cache
      * In case of none returned, value will be ignored by cache
      */
    def getOrUpdateResourceOpt(key: K)(value: => Resource[F, Option[V]])(implicit F: MonadCancel[F, Throwable]): F[Option[V]] = {
      self
        .getOrUpdateOpt1(key) {
          value
            .allocated
            .flatMap {
              case (Some(a), release) =>
                (a, a, release.some)
                  .some
                  .pure[F]
              case (None, release)    =>
                release.as { none[(V, V, Option[F[Unit]])] }
            }
        }
        .flatMap {
          case Some(Right(Right(a))) => a.some.pure[F]
          case Some(Right(Left(a)))  => a.map { _.some }
          case Some(Left(a))         => a.some.pure[F]
          case None                  => none[V].pure[F]
        }
    }
  }

  implicit class CacheResourceOps[F[_], K, V](val self: Resource[F, Cache[F, K, V]]) extends AnyVal {

    @deprecated("use `Cache[F, K, V].withFence`", "4.1.1")
    def withFence(implicit F: Concurrent[F]): Resource[F, Cache[F, K, V]] = CacheFenced.of(self)
  }
}