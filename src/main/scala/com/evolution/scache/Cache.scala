package com.evolution.scache

import cats.effect.kernel.MonadCancel
import cats.effect.{Concurrent, Resource, Temporal}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.{Applicative, Functor, Hash, Monad, MonadThrow, Monoid, Parallel, ~>}
import cats.kernel.CommutativeMonoid
import com.evolution.scache.Cache.Directive
import com.evolutiongaming.catshelper.CatsHelper.*
import com.evolutiongaming.catshelper.{MeasureDuration, Runtime}

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
    * @param key
    *   The key to return the value for.
    * @return
    *   - If the key is already in the cache then `F[_]` will complete to
    *     `Some(v)`, where `v` is a value associated with the key.
    *   - If the new value is loading (as result of [[#getOrUpdate]] or
    *     implementation-specific refresh), then `F[_]` will not complete until
    *     the value is fully loaded.
    *   - `F[_]` will complete to [[scala.None]] if there is no `key` present in
    *     the cache.
    */
  def get(key: K): F[Option[V]]

  /** Gets a value for specific key.
    *
    * The point of this method, comparing to [[#get]] is that it does not wait
    * if the value for a specific key is still loading, allowing the caller to
    * not block while waiting for it.
    *
    * @param key
    *   The key to return the value for.
    * @return
    *   - If the key is already in the cache then `F[_]` will complete to
    *     `Some(Right(v))`, where `v` is a value associated with the key.
    *   - If the new value is loading (as result of [[#getOrUpdate]] or
    *     implementation-specific refresh), then `F[_]` will complete to
    *     `Some(Left(io))`, where `io` will not complete until the value is
    *     fully loaded.
    *   - `F[_]` will complete to [[scala.None]] if there is no `key` present in
    *     the cache.
    */
  def get1(key: K): F[Option[Either[F[V], V]]]

  /** Gets a value for specific key, or loads it using the provided function.
    *
    * The method does not run `value` concurrently for the same key. I.e. if
    * `value` takes a time to be completed, and [[#getOrUpdate]] is called
    * several times, then the consequent calls will not cause `value` to be
    * called, but will wait for the first one to complete.
    *
    * @param key
    *   The key to return the value for.
    * @param value
    *   The function to run to load the missing value with.
    *
    * @return
    *   - If the key is already in the cache then `F[_]` will complete to the
    *     value associated with the key.
    *   - `F[_]` will complete to the value loaded by `value` function if there
    *     is no `key` present in the cache.
    *   - If the new value is loading (as result of this or another
    *     [[#getOrUpdate]] call, or implementation-specific refresh), then
    *     `F[_]` will not complete until the value is fully loaded.
    */
  def getOrUpdate(key: K)(value: => F[V]): F[V]

  /** Gets a value for specific key, or loads it using the provided function.
    *
    * The point of this method, comparing to [[#getOrUpdate]] is that it does
    * not wait if value for a key is still loading, allowing the caller to not
    * block while waiting for the result.
    *
    * It also allows some additional functionality similar to
    * [[#put(key:K,value:V,release:Option[*]].
    *
    * The method does not run `value` concurrently for the same key. I.e. if
    * `value` takes a time to be completed, and [[#getOrUpdate1]] is called
    * several times, then the consequent calls will not cause `value` to be
    * called, but will wait for the first one to complete.
    *
    * The `value` is only called if `key` is not found, the tuple elements will
    * be used like following:
    *   - `A` will be returned by [[#getOrUpdate1]] to differentiate from the
    *     case when the value is already there,
    *   - `V` will be put to the cache,
    *   - `Release`, if present, will be called when this value is removed from
    *     the cache.
    *
    * Note: this method is meant to be used where [[cats.effect.Resource]] is
    * not convenient to use, i.e. when integration with legacy code is required
    * or for internal implementation. For all other cases it is recommended to
    * use [[Cache.CacheOps#getOrUpdateResource]] instead as more human-readable
    * alternative.
    *
    * @param key
    *   The key to return the value for.
    * @param value
    *   The function to run to load the missing value with.
    *
    * @tparam A
    *   Arbitrary type of a value to return in case key was not present in a
    *   cache.
    *
    * @return
    *   Either `A` passed as argument, if `key` was not found in cache, or
    *   `Either[F[V], V]` that represents loading or loaded value, if `key` is
    *   already in the cache.
    */
  def getOrUpdate1[A](key: K)(value: => F[(A, V, Option[Release])]): F[Either[A, Either[F[V], V]]]

  /** Gets a value for specific key, or tries to load it.
    *
    * The difference between this method and [[#getOrUpdate]] is that this one
    * allows the loading function to fail finding the value, i.e. return
    * [[scala.None]].
    *
    * @param key
    *   The key to return the value for.
    * @param value
    *   The function to run to load the missing value with.
    *
    * @return
    *   The same semantics applies as in [[#getOrUpdate]], except that the
    *   method may return [[scala.None]] in case `value` completes to
    *   [[scala.None]].
    */
  def getOrUpdateOpt(key: K)(value: => F[Option[V]]): F[Option[V]]

  /** Puts a value into cache under specific key.
    *
    * If the value is already being loaded (using [[#getOrUpdate]] method?) then
    * the returned `F[_]` will wait for it to fully load, and then overwrite it.
    *
    * @param key
    *   The key to store value for.
    * @param value
    *   The new value to put into the cache.
    *
    * @return
    *   A previous value is returned if it was already added into the cache,
    *   [[scala.None]] otherwise. The returned value is wrapped into `F[_]`
    *   twice, because outer `F[_]` will complete when the value is put into
    *   cache, but the second when `release` function passed to
    *   [[#put(key:K,value:V,release:Cache*]] completes, i.e. the underlying
    *   resource is fully released.
    */
  def put(key: K, value: V): F[F[Option[V]]]

  /** Puts a value into cache under specific key.
    *
    * If the value is already being loaded (using [[#getOrUpdate]] method?) then
    * the returned `F[_]` will wait for it to fully load, and then overwrite it.
    *
    * @param key
    *   The key to store value for.
    * @param value
    *   The new value to put into the cache.
    * @param release
    *   The function to call when the value is removed from the cache.
    *
    * @return
    *   A previous value is returned if it was already added into the cache,
    *   [[scala.None]] otherwise. The returned value is wrapped into `F[_]`
    *   twice, because outer `F[_]` will complete when the value is put into
    *   cache, but the second when `release` function passed to
    *   [[#put(key:K,value:V,release:Cache*]] completes, i.e. the underlying
    *   resource is fully released.
    */
  def put(key: K, value: V, release: Release): F[F[Option[V]]]

  /** Puts a value into cache under specific key.
    *
    * If the value is already being loaded (using [[#getOrUpdate]] method?) then
    * the returned `F[_]` will wait for it to fully load, and then overwrite it.
    *
    * @param key
    *   The key to store value for.
    * @param value
    *   The new value to put into the cache.
    * @param release
    *   The function to call when the value is removed from the cache.
    *   No function will be called if it is set to [[scala.None]].
    *
    * @return
    *   A previous value is returned if it was already added into the cache,
    *   [[scala.None]] otherwise. The returned value is wrapped into `F[_]`
    *   twice, because outer `F[_]` will complete when the value is put into
    *   cache, but the second when `release` function passed to
    *   [[#put(key:K,value:V,release:Cache*]] completes, i.e. the underlying
    *   resource is fully released.
    */
  def put(key: K, value: V, release: Option[Release]): F[F[Option[V]]]

  /** Atomically modify a value under specific key.
    *
    * Allows to make a decision regarding value update based on the present value (or its absence),
    * and express it as either `Put`, `Ignore`, or `Remove` directive.
    *
    * It will try to calculate `f` and apply resulting directive until it succeeds.
    *
    * In case of `Put` directive, it is guaranteed that the value is written in cache,
    * and that it replaced exactly the value passed to `f`.
    *
    * In case of `Remove` directive, it is guaranteed that the key was removed
    * when it contained exactly the value passed to `f`.
    *
    * @param key
    *    The key to modify value for.
    * @param f
    *    Function that accepts current value found in cache (or None, if it's absent), and returns
    *    a directive expressing a desired operation on the value, as well as an arbitrary output value of type `A`
    * @return
    *    Output value returned by `f`, and an optional effect representing an ongoing release of the value
    *    that was removed from cache as a result of the modification (e.g.: in case of `Put` or `Remove` directives).
    */
  def modify[A](key: K)(f: Option[V] => (A, Directive[F, V])): F[(A, Option[F[Unit]])]

  /** Checks if the value for the key is present in the cache.
    *
    * @return
    *   `true` if either loaded or loading value is present in the cache.
    */
  def contains(key: K): F[Boolean]

  /** Calculates the size of the cache including both loaded and loading keys.
    *
    * May iterate over all of keys for map-bazed implementation, hence should be
    * used with care on very large caches.
    *
    * @return
    *   current size of the cache.
    */
  def size: F[Int]

  /** Returns set of the keys present in the cache, either loaded or loading.
    *
    * @return
    *   keys present in the cache, either loaded or loading.
    */
  def keys: F[Set[K]]

  /** Returns map representation of the cache.
    *
    * Warning: this might be an expensive call to make.
    *
    * @return
    *   All keys and values in the cache put into map. Both loaded and loading
    *   values will be wrapped into `F[V]`.
    */
  def values: F[Map[K, F[V]]]

  /** Returns map representation of the cache.
    *
    * The different between this method and [[#values]] is that loading values
    * are not wrapped into `F[V]` and decision if it is worth to wait for their
    * completion is left to the caller discretion.
    *
    * Warning: this might be an expensive call to make.
    *
    * @return
    *   All keys and values in the cache put into map. Loaded values are
    *   returned as `Right(v)`, while loading ones are represented by
    *   `Left(F[V])`.
    */
  def values1: F[Map[K, Either[F[V], V]]]

  /** Removes a key from the cache, and also calls a release function.
    *
    * @return
    *   A stored value is returned if such was present in the cache,
    *   [[scala.None]] otherwise. The returned value is wrapped into `F[_]`
    *   twice, because outer `F[_]` will complete when the value is put into
    *   cache, but the second when `release` function passed to
    *   [[#put(key:K,value:V,release:Cache*]] completes, i.e. the underlying
    *   resource is fully released.
    */
  def remove(key: K): F[F[Option[V]]]

  /** Removes all the keys and their respective values from the cache.
    *
    * Both loaded and loading values are removed, and `release` function is
    * called on them if present. The call does not cancel the loading values,
    * but waits until these are fully loaded, instead.
    *
    * @return
    *   The returned `Unit` is wrapped into `F[_]` twice, because outer
    *   `F[Released]` will complete when the value is put into cache, but the
    *   second `Released = F[Unit]` when `release` function passed to
    *   [[#put(key:K,value:V,release:Cache*]] completes, i.e. the underlying
    *   resource is fully released.
    */
  def clear: F[Released]

  /** Aggregate all keys and values present in the cache to something else.
    *
    * Example: calculate sum of all loaded [[scala.Int]] values:
    * {{{
    * cache.foldMap {
    *   case (key, Right(loadedValue)) => loadedValue.pure[F]
    *   case (key, Left(pendingValue)) => 0.pure[F]
    * }
    * }}}
    *
    * @tparam A
    *   Type to map the key/values to, and aggregate with. It requires
    *   [[cats.kernel.CommutativeMonoid]] to be present to be able to sum up the
    *   values, without having a guarantee about the order of the values being
    *   aggregates as the order may be random depending on a cache
    *   implementation.
    *
    * @return
    *   Result of the aggregation, i.e. all mapped values combined using passed
    *   [[cats.kernel.CommutativeMonoid]].
    */
  def foldMap[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]): F[A]

  /** Aggregate all keys and values present in the cache to something else.
    *
    * The difference between this method and [[#foldMap]] is that this one may
    * perform the work in parallel.
    *
    * Example: calculate sum of all loading [[scala.Int]] values in parallel:
    * {{{
    * cache.foldMapPar {
    *   case (key, Right(loadedValue)) => 0.pure[F]
    *   case (key, Left(pendingValue)) => pendingValue
    * }
    * }}}
    *
    * @tparam A
    *   Type to map the key/values to, and aggregate with. It requires
    *   [[cats.kernel.CommutativeMonoid]] to be present to be able to sum up the
    *   values, without having a guarantee about the order of the values being
    *   aggregates as the order may be random depending on a cache
    *   implementation.
    *
    * @return
    *   Result of the aggregation, i.e. all mapped values combined using passed
    *   [[cats.kernel.CommutativeMonoid]].
    */
  def foldMapPar[A: CommutativeMonoid](f: (K, Either[F[V], V]) => F[A]): F[A]
}

object Cache {

  sealed trait Directive[+F[_], +V]
  object Directive {
    final case class Put[F[_], V](value: V, release: Option[F[Unit]]) extends Directive[F, V]
    final case object Remove extends Directive[Nothing, Nothing]
    final case object Ignore extends Directive[Nothing, Nothing]
  }

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

      def modify[A](key: K)(f: Option[V] => (A, Directive[F, V])): F[(A, Option[F[Unit]])] =
        (f(None)._1, none[F[Unit]]).pure[F]

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
    * {{{
    * Cache.loading[F, String, User]
    * }}}
    *
    * @return
    *   A new instance of a cache wrapped into [[cats.effect.Resource]]. Note,
    *   that [[Cache#clear]] method will be called on underlying cache when
    *   resource is released to make sure all resources stored in a cache are
    *   also released.
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
    * {{{
    * Cache.loading[F, String, User](partitions = 8)
    * }}}
    *
    * @return
    *   A new instance of a cache wrapped into [[cats.effect.Resource]]. Note,
    *   that [[Cache#clear]] method will be called on underlying cache when
    *   resource is released to make sure all resources stored in a cache are
    *   also released.
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
    * {{{
    * Cache.loading[F, String, User](partitions = None)
    * }}}
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
    *
    * @return
    *   A new instance of a cache wrapped into [[cats.effect.Resource]]. Note,
    *   that [[Cache#clear]] method will be called on underlying cache when
    *   resource is released to make sure all resources stored in a cache are
    *   also released.
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
    * The underlying storage implementation is the same as in
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
    * {{{
    * Cache.expiring[F, String, User](
    *   config = ExpiringCache.Config(expireAfterRead = 1.minute),
    *   partitions = None,
    * )
    * }}}
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
    *
    * @return
    *   A new instance of a cache wrapped into [[cats.effect.Resource]]. Note,
    *   that [[Cache#clear]] method will be called on underlying cache when
    *   resource is released to make sure all resources stored in a cache are
    *   also released.
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

        def modify[A](key: K)(f: Option[V] => (A, Directive[G, V])): G[(A, Option[G[Unit]])] = {
          val adaptedF: Option[V] => (A, Directive[F, V]) = f(_) match {
            case (a, put: Directive.Put[G, V]) => (a, Directive.Put(put.value, put.release.map(gf(_))))
            case (a, Directive.Ignore) => (a, Directive.Ignore)
            case (a, Directive.Remove) => (a, Directive.Remove)
          }
          fg {
            self
              .modify(key)(adaptedF)
              .map { case (a, release) => (a, release.map(fg(_)))}
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

    /** Prevents adding new keys with `release` after cache itself was released.
      *
      * This may be useful, for example, to prevent dangling cache references to
      * be filled instead of an intended instance.
      */
    def withFence(implicit F: Concurrent[F]): Resource[F, Cache[F, K, V]] = CacheFenced.of(self)

    /** Gets a value for specific key or uses another value.
      *
      * The semantics is exactly the same as in [[Cache#get]].
      *
      * Warning: The value passed as a second argument may only be returned, and
      * never put into cache. If putting a value into cache is required, then
      * [[Cache#getOrUpdate]] should be called instead.
      *
      * @param key
      *   The key to return the value for.
      * @param value
      *   The function to run to get the missing value with.
      *
      * @return
      *   The same semantics applies as in [[Cache#getOrUpdate]], except that in
      *   this method there is no possibility to get [[scala.None]].
      */
    def getOrElse(key: K, value: => F[V])(implicit F: Monad[F]): F[V] = {
      self
        .get(key)
        .flatMap {
          case Some(a) => a.pure[F]
          case None    => value
        }
    }

    /** Gets a value for specific key, or loads it using a specified function.
      *
      * The difference between this method and [[Cache#getOrUpdate1]] is that
      * this one does not differentiate between loading or loaded values present
      * in a cache. If the value is still loading, `F[_]` will not complete
      * until is is fully loaded.
      *
      * Also this method is meant to be used where [[cats.effect.Resource]] is
      * not convenient to use, i.e. when integration with legacy code is
      * required or for internal implementation. For all other cases it is
      * recommended to use [[#getOrUpdateResource]] instead as more
      * human-readable alternative.
      *
      * @param key
      *   The key to return the value for.
      * @param value
      *   The function to run to load the missing value with.
      *
      * @tparam A
      *   Arbitrary type of a value to return in case key was not present in a
      *   cache.
      *
      * @return
      *   The same semantics applies as in [[Cache#getOrUpdate1]], except that
      *   in this method `F[_]` will only complete when the value is fully
      *   loaded.
      */
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

    /** Gets a value for specific key, or tries to load it.
      *
      * The difference between this method and [[Cache#getOrUpdate1]] is that
      * this one allows the loading function to fail finding the value, i.e.
      * return [[scala.None]].
      *
      *
      * Also this method is meant to be used where [[cats.effect.Resource]] is
      * not convenient to use, i.e. when integration with legacy code is
      * required or for internal implementation. For all other cases it is
      * recommended to use [[#getOrUpdateResourceOpt]] instead as more
      * human-readable alternative.
      *
      * @param key
      *   The key to return the value for.
      * @param value
      *   The function to run to load the missing value with.
      *
      * @tparam A
      *   Arbitrary type of a value to return in case key was not present in a
      *   cache.
      *
      * @return
      *   The same semantics applies as in [[Cache#getOrUpdate1]], except that
      *   the method may return [[scala.None]] in case `value` completes to
      *   [[scala.None]].
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

    /** Gets a value for specific key, or loads it using the provided function.
      *
      * The difference between this method and [[Cache#getOrUpdate]] is that it
      * accepts [[cats.effect.Resource]] as a value parameter and releases it
      * when the value is removed from cache.
      *
      * The method does not run `value` concurrently for the same key. I.e. if
      * `value` takes a time to be completed, and [[#getOrUpdateResource]] is
      * called several times, then the consequent calls will not cause `value`
      * to be called, but will wait for the first one to complete.
      *
      * @param key
      *   The key to return the value for.
      * @param value
      *   The function to run to load the missing resource with.
      *
      * @return
      *   - If the key is already in the cache then `F[_]` will complete to the
      *     value associated with the key.
      *   - `F[_]` will complete to the value loaded by `value` function if
      *     there is no `key` present in the cache.
      *   - If the new value is loading (as result of this or another
      *     [[#getOrUpdateResource]] call, or implementation-specific refresh),
      *     then `F[_]` will not complete until the value is fully loaded.
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

    /** Gets a value for specific key, or tries to load it.
      *
      * The difference between this method and [[#getOrUpdateResource]] is
      * that this one allows the loading function to fail finding the value,
      * i.e. return [[scala.None]].
      *
      * @param key
      *   The key to return the value for.
      * @param value
      *   The function to run to load the missing value with.
      *
      * @return
      *   The same semantics applies as in [[#getOrUpdateResource]], except that
      *   the method may return [[scala.None]] in case `value` completes to
      *   [[scala.None]]. The resource will be released normally even if `None`
      *   is returned.
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

    /** Like `modify`, but doesn't pass through any return value.
     *
     * @return
     *   If this `update` replaced an existing value,
     *   will return `Some` containing an effect representing release of that value.
     */
    def update(key: K)(f: Option[V] => Directive[F, V])(implicit F: Functor[F]): F[Option[F[Unit]]] =
      self.modify(key)(() -> f(_)).map(_._2)

    /** Like `modify`, but `f` is only applied if there is a value present in cache,
     * and the result is always replacing the old value.
     *
     * @return
     *   `true` if value was present, and was subsequently replaced.
     *   `false` if there was no value present.
     */
    def updatePresent(key: K)(f: V => V)(implicit F: Functor[F]): F[Boolean] =
      self.modify[Boolean](key) {
        case Some(value) => (true, Directive.Put(f(value), None))
        case None => (false, Directive.Ignore)
      } map(_._1)

    /** Like `update`, but `f` has an option to return `None`, in which case value will not be changed.
     *
     * @return
     *   `true` if value was present and was subsequently replaced.
     *   `false` if there was no value present, or it was not replaced.
     */
    def updatePresentOpt(key: K)(f: V => Option[V])(implicit F: Functor[F]): F[Boolean] =
      self.modify[Boolean](key) {
        case Some(value) => f(value).fold[(Boolean, Directive[F, V])](false -> Directive.Ignore)(v => true -> Directive.Put(v, None))
        case None => (false, Directive.Ignore)
      } map(_._1)

    /** Like `put`, but based on `modify`, and guarantees that as a result of the operation the value was in fact
     * written in cache. Will be slower than a regular `put` in situations of high contention.
     *
     * @return
     *   If this `putStrict` replaced an existing value, will return `Some` containing the old value
     *   and an effect representing release of that value.
     */
    def putStrict(key: K, value: V)(implicit F: Applicative[F]): F[Option[(V, F[Unit])]] =
      self.modify[Option[V]](key)((_, Directive.Put(value, None))).map(_.tupled)

    /** Like `putStrict`, but with `release` part of the new value.
     *
     * @return
     *   If this `putStrict` replaced an existing value, will return `Some` containing the old value
     *   and an effect representing release of that value.
     */
    def putStrict(key: K, value: V, release: self.type#Release)(implicit F: Applicative[F]): F[Option[(V, F[Unit])]] =
      self.modify[Option[V]](key)((_, Directive.Put(value, release.some))).map(_.tupled)
  }
}
