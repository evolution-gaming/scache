# SCache
[![Build Status](https://github.com/evolution-gaming/scache/workflows/CI/badge.svg)](https://github.com/evolution-gaming/scache/actions?query=workflow%3ACI)
[![Coverage Status](https://coveralls.io/repos/evolution-gaming/scache/badge.svg)](https://coveralls.io/r/evolution-gaming/scache)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/d6da847f1228485e91525112112fb86b)](https://www.codacy.com/app/evolution-gaming/scache?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/scache&amp;utm_campaign=Badge_Grade)
 [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.evolution/scache_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.evolution/scache_2.13)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)

## Key features

* Auto loading of missing values
* Expiry of not used records
* Deleting oldest values in case of exceeding max size
* Tagless Final
* Partition entries by `hashCode` into multiple caches in order to avoid thread contention for some corner cases  

## Cache.scala 

```scala
trait Cache[F[_], K, V] {

  def get(key: K): F[Option[V]]

  def getOrElse(key: K, default: => F[V]): F[V]

  /**
    * Does not run `value` concurrently for the same key
    */
  def getOrUpdate(key: K)(value: => F[V]): F[V]

  /**
    * Does not run `value` concurrently for the same key
    * Releasable.release will be called upon key removal from the cache
    */
  def getOrUpdateReleasable(key: K)(value: => F[Releasable[F, V]]): F[V]

  /**
    * @return previous value if any, possibly not yet loaded
    */
  def put(key: K, value: V): F[F[Option[V]]]


  def put(key: K, value: V, release: F[Unit]): F[F[Option[V]]]


  def size: F[Int]


  def keys: F[Set[K]]

  /**
    * Might be an expensive call
    */
  def values: F[Map[K, F[V]]]

  /**
    * @return previous value if any, possibly not yet loaded
    */
  def remove(key: K): F[F[Option[V]]]


  /**
    * Removes loading values from the cache, however does not cancel them
    */
  def clear: F[F[Unit]]
}
```

## SerialMap.scala

```scala
trait SerialMap[F[_], K, V] {

  def get(key: K): F[Option[V]]

  def getOrElse(key: K, default: => F[V]): F[V]

  /**
    * Does not run `value` concurrently for the same key
    */
  def getOrUpdate(key: K, value: => F[V]): F[V]

  def put(key: K, value: V): F[Option[V]]

  /**
    * `f` will be run serially for the same key, entry will be removed in case of `f` returns `none`
    */
  def modify[A](key: K)(f: Option[V] => F[(Option[V], A)]): F[A]

  /**
    * `f` will be run serially for the same key, entry will be removed in case of `f` returns `none`
    */
  def update[A](key: K)(f: Option[V] => F[Option[V]]): F[Unit]

  def size: F[Int]

  def keys: F[Set[K]]

  /**
    * Might be an expensive call
    */
  def values: F[Map[K, V]]

  def remove(key: K): F[Option[V]]

  def clear: F[Unit]
}
```

## Setup

```scala
libraryDependencies += "com.evolution" %% "scache" % "4.3.1.1"
```

## ExpiringCache

![Behaviour of Expiring Cache](ExpiringCache.png)

### Recommendations

* There is no use to make refresh.interval bigger than expireAfterWrite. It's just the waste of resources.
* Touch, despite its name, is not called after refresh.
* expireAfterWrite, despite its name, is calculated from date of creation, not time of update.