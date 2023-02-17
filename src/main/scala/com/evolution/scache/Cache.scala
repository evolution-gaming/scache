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

import scala.util.control.NoStackTrace

/** Tagless Final implementation of a cache interface.
  *
  * Most developers using the library may want to use [[Cache#expiring]] to
  * construct the cache, though, if element expiration is not required, then
  * it might be useful to use
  * [[Cache#loading[F[_],K,V](partitions:Option[Int])*]] instead.
  *
  * @tparam F
  *   Effect to be used in effectful methods such as [[#get]].
  * @tparam K
  *   Key type. While there is no restriction / context bounds on this type
  *   parameter, the implementation is expected to abuse the fact that it is
  *   possible to call `hashCode` and `==` on any object in JVM. If performance
  *   is important, it is recommeded to limit `K` to the types where these
  *   operations are fast such as `String`, `Integer` or a case class.
  * @tparam V
  *   Value type.
  */
trait Cache[F[_], K, V] {

  type Release = F[Unit]

  type Released = F[Unit]

  /** Gets a value for specific key.
    *
    *   - If the new value is loading (as result of [[#getOrUpdate]] or
    *     implementation-specific refresh), then `F[_]` will not complete until
    *     the value is fully loaded.
    *   - `F[_]` will complete to `None` if there is no `key` present in the
    *     cache.
    */
  def get(key: K): F[Option[V]]

  /** Gets a value for specific key.
    *
    * The point of this method, comparing to [[#get]] is that it does not wait
    * if the value for a specific key is still loading, allowing the caller to
    * not block while waiting for it.
    *
    *   - If the value is already in the cache then `F[_]` will complete to
    *     `Some(Right(v))`.
    *   - If the new value is loading (as result of [[#getOrUpdate]] or
    *     implementation-specific refresh), then `F[_]` will complete to
    *     `Some(Left(io))`, where `io` will not complete until the value is
    *     fully loaded.
    *   - `F[_]` will complete to `None` if there is no `key` present in the
    *     cache.
    */
  def get1(key: K): F[Option[Either[F[V], V]]]

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

  /** Creates an always-empty implementation of cache.
    *
    * The implementation *almost* always returns [[scala.None]] regardess the
    * key. The notable exception are [[Cache#getOrUpdate]],
    * [[Cache#getOrUpdate1]] and [[Cache#getOrUpdateOpt]] methods, which return
    * the value passed to them to ensure the consistent behavior (i.e. it could
    * be a suprise if someone calls [[Cache#getOrUpdateOpt]] with [[scala.Some]]
    * and gets [[scala.None]] as a result).
    *
    * It is meant to be used in tests, or as a stub in the code where cache
    * should be disabled.
    */
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

  /** Creates a cache implementation, which is able to load the missing values.
    *
    * Same as [[[[#loading[F[_],K,V](partitions:Int)*]]]], but with number of
    * paritions determined automatically using passed `Runtime` implementation.
    *
    * Minimal usage example:
    * {{
    * Cache.loading[F, String, User]
    * }}
    */
  def loading[F[_]: Concurrent: Parallel: Runtime, K, V]: Resource[F, Cache[F, K, V]] = {
    loading(none)
  }

  /** Creates a cache implementation, which is able to load the missing values.
    *
    * Same as [[#loading[F[_],K,V](partitions:Option[Int])*]], but without the
    * need to use `Option`.
    *
    * Minimal usage example:
    * {{
    * Cache.loading[F, String, User](partitions = 8)
    * }}
    */
  def loading[F[_]: Concurrent: Parallel: Runtime, K, V](partitions: Int): Resource[F, Cache[F, K, V]] = {
    loading(partitions.some)
  }

  /** Creates a cache implementation, which is able to load the missing values.
    *
    * To speed the operations, the cache may use several partitions each of whom
    * may be accessed in parallel.
    *
    * Note, that the values getting into this cache never expire, i.e. the cache
    * will grow indefinetely unless [[Cache#remove]] is called. See
    * [[#expiring]] for the implementation, which allows automatic expriation of
    * the values.
    *
    * Here is a short description of why some of the context bounds are required
    * on `F[_]`:
    *   - [[cats.Parallel]] is required for an efficient [[Cache#foldMapPar]]
    *     implementation whenever cache partitioning is used. Cache partitioning
    *     itself allows splitting underlying cache into multiple partitions, so
    *     there is no contention on a single [[cats.effect.Ref]] when cache need
    *     to be updated.
    *   - `Runtime` is used to determine optimal number of partitions based on
    *     CPU count if the value is not provided as a parameter.
    *   - [[cats.effect.Sync]] (which comes as part of
    *     [[cats.effect.Concurrent]]), allows internal structures using
    *     [[cats.effect.Ref]] and [[cats.effect.Deferred]] to be created.
    *   - [[cats.effect.Concurrent]], allows `release` parameter in
    *     [[Cache#put(key:K,value:V,release:Cache*]] and [[Cache#getOrUpdate1]]
    *     methods to be called in background without waiting for release to be
    *     completed.
    *
    * Minimal usage example:
    * {{
    * Cache.loading[F, String, User](partitions = None)
    * }}
    *
    * @tparam F
    *   Effect type. See [[Cache]] for more details.
    * @tparam K
    *   Key type. See [[Cache]] for more details.
    * @tparam V
    *   Value type. See [[Cache]] for more details.
    *
    * @param partitions
    *   Number of partitions to use, or [[scala.None]] in case number of
    *   partitions should be determined automatically using passed `Runtime`
    *   implementation.
    */
  def loading[F[_]: Concurrent: Parallel: Runtime, K, V](partitions: Option[Int] = None): Resource[F, Cache[F, K, V]] = {

    implicit val hash: Hash[K] = Hash.fromUniversalHashCode[K]

    val result = for {
      nrOfPartitions <- partitions
        .map { _.pure[F] }
        .getOrElse { NrOfPartitions[F]() }
        .toResource
      cache           = LoadingCache.of(LoadingCache.EntryRefs.empty[F, K, V])
      partitions     <- Partitions.of[Resource[F, _], K, Cache[F, K, V]](nrOfPartitions, _ => cache)
    } yield {
      fromPartitions(partitions)
    }
    result.breakFlatMapChain
  }

  /** Creates a cache implementation, which is able remove the stale values.
    *
    * The undelying storage implementation is the same as in
    * [[#loading[F[_],K,V](partitions:Option[Int])*]], but the expiration
    * routines are added on top of it.
    *
    * Besides a value expiration leading to specific key being removed from the
    * cache, the implementation is capable of _refreshing_ the values instead of
    * removing them, which might be useful if the cache is used as a wrapper for
    * setting or configuration service. The feature is possible to configure
    * using `config` parameter.
    *
    * In adddition to context bounds used in
    * [[#loading[F[_],K,V](partitions:Option[Int])*]], this implementation also
    * adds [[cats.effect.Clock]] (as part of [[cats.effect.Temporal]]), to have
    * the ability to schedule cache clean up in a concurrent way.
    *
    * Minimal usage example:
    * {{
    * Cache.expiring[F, String, User](
    *   config = ExpiringCache.Config(expireAfterRead = 1.minute),
    *   partitions = None,
    * )
    * }}
    *
    * @tparam F
    *   Effect type. See [[#loading[F[_],K,V](partitions:Option[Int])*]] and
    *   [[Cache]] for more details.
    * @tparam K
    *   Key type. See [[Cache]] for more details.
    * @tparam V
    *   Value type. See [[Cache]] for more details.
    *
    * @param config
    *   Cache configuration. See [[ExpiringCache.Config]] for more details on
    *   what parameters could be configured.
    * @param partitions
    *   Number of partitions to use, or [[scala.None]] in case number of
    *   partitions should be determined automatically using passed `Runtime`
    *   implementation.
    */
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
      fromPartitions(partitions)
    }

    result.breakFlatMapChain
  }

  /** Creates [[Cache]] interface to a set of precreated caches.
    *
    * This method is required to use common partitioning implementation for
    * various caches and is not intended to be called directly. Cache
    * partitioning itself allows splitting underlying cache into multiple
    * partitions, so there is no contention on a single [[cats.effect.Ref]] when
    * cache need to be updated.
    *
    * It is only left public for sake of backwards compatibility.
    *
    * Please consider using either
    * [[#loading[F[_],K,V](partitions:Option[Int])*]] or [[#expiring]] instead.
    *
    * Here is a short description of why some of the context bounds are required
    * on `F[_]`:
    *   - [[cats.MonadError]] is required to throw an error in case partitioning
    *     function passed as part of [[Partitions]] does not return any values.
    *   - [[cats.Parallel]] is required for an efficient [[Cache#foldMapPar]]
    *     implementation.
    */
  def fromPartitions[F[_]: MonadThrow: Parallel, K, V](partitions: Partitions[K, Cache[F, K, V]]): Cache[F, K, V] = {
    PartitionedCache(partitions)
  }

  private[scache] abstract class Abstract0[F[_], K, V] extends Cache[F, K, V] { self =>

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

    def withFence(implicit F: Concurrent[F]): Resource[F, Cache[F, K, V]] = CacheFenced.of(self)

    def getOrElse(key: K, value: => F[V])(implicit F: Monad[F]): F[V] = {
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
}
