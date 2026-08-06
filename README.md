# SCache
[![Build Status](https://github.com/evolution-gaming/scache/workflows/CI/badge.svg)](https://github.com/evolution-gaming/scache/actions?query=workflow%3ACI)
[![Coverage Status](https://coveralls.io/repos/evolution-gaming/scache/badge.svg)](https://coveralls.io/r/evolution-gaming/scache)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/c44790f3e44a495488141d9eed4aa757)](https://www.codacy.com/gh/evolution-gaming/scache/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/scache&amp;utm_campaign=Badge_Grade)
[![Latest version](https://img.shields.io/badge/version-click-blue)](https://evolution.jfrog.io/artifactory/api/search/latestVersion?g=com.evolution&a=scache_2.13&repos=public)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)

## Key features

* Available for: Scala 2.13.x, 3.3.x and later
* Auto loading of missing values
* Expiry of not used records
* Deleting oldest values in case of exceeding max size
* Tagless Final
* Partition entries by `hashCode` into multiple caches in order to avoid thread contention for some corner cases

## Introduction

`Cache` is a main entry point towards `scache` library. Most users may want to
call `Cache#expiring` method to get the instance of the trait. The
documentation could be found in source code of
[Cache.scala](src/main/scala/com/evolution/scache/Cache.scala) and also at
[javadoc.io](https://javadoc.io/doc/com.evolution/scache_2.13/latest/com/evolution/scache/Cache$.html).

See [Setup](https://github.com/evolution-gaming/scache#setup) for more details
on how to add the library itself.

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

`scache`, along with its dependencies, is available on Evolution's JFrog Artifactory. That is why one needs to include
a dependency on https://github.com/evolution-gaming/sbt-artifactory-plugin.

```scala
addSbtPlugin("com.evolution" % "sbt-artifactory-plugin" % "0.0.2")

libraryDependencies += "com.evolution" %% "scache" % "<latest version from badge>"
```

## ExpiringCache

![Behaviour of Expiring Cache](ExpiringCache.png)

### Recommendations

* There is no use to make refresh.interval bigger than expireAfterWrite. It's just the waste of resources.
* Touch, despite its name, is not called after refresh.
* expireAfterWrite, despite its name, is calculated from date of creation, not time of update.

## Benchmarks

The `benchmark` module holds a JMH benchmark of the cache operations under contention. One
invocation is the whole workload, 8 fibers running 20000 operations each against a key space of
10000, so the reported number is cache operations per second with the contention included.

```scala
// everything, around 10 minutes
sbt "benchmark/Jmh/run"

// one scenario, one flavor
sbt "benchmark/Jmh/run -p flavor=partitioned .*getOrUpdateHitRandomKeys.*"

// longer run when the defaults are too noisy to tell two numbers apart
sbt "benchmark/Jmh/run -wi 5 -i 10 -r 5s"
```

`flavor` picks how the cache is put together: `single` is one unpartitioned `LoadingCache`,
`partitioned` is `Cache.loading`, `expiring` is `Cache.expiring` with the expiration set far enough
away not to interfere.

The whole suite is kept under ten minutes, which is one warmup and five measurement iterations per
scenario. That is enough to compare implementations or spot a regression, not to argue about a few
percent, and some of the scenarios below are visibly noisy.

Measured on 12 cores, JDK 25, Scala 2.13.18, in millions of operations per second, higher is better:

| Scenario | single | partitioned | expiring |
|---|---:|---:|---:|
| `getOrUpdate`, insert distinct keys | 2.32 ± 1.38 | 2.40 ± 0.44 | 1.84 ± 0.30 |
| `getOrUpdate`, hit random keys | 12.71 ± 1.43 | 11.37 ± 1.47 | 9.22 ± 3.27 |
| `getOrUpdate`, hit single hot key | 11.65 ± 1.56 | 12.47 ± 1.66 | 10.52 ± 0.47 |
| `get`, hit random keys | 28.21 ± 0.33 | 23.54 ± 1.25 | 13.14 ± 0.91 |
| `get1`, hit random keys | 23.52 ± 0.48 | 20.51 ± 0.99 | 13.18 ± 0.43 |
| `contains`, random keys | 34.11 ± 36.63 | 37.59 ± 2.81 | 34.21 ± 1.04 |
| `put`, insert distinct keys | 10.91 ± 2.85 | 10.31 ± 2.70 | 9.05 ± 1.42 |
| `put`, replace random keys | 9.93 ± 0.68 | 8.76 ± 0.42 | 8.23 ± 0.60 |
| `modify`, insert distinct keys | 11.77 ± 5.37 | 11.42 ± 1.74 | 10.91 ± 2.24 |
| `modify`, update random keys | 11.21 ± 1.42 | 9.26 ± 3.56 | 10.42 ± 2.03 |
| `remove` and `put`, random keys | 4.05 ± 0.11 | 3.87 ± 0.16 | 2.72 ± 2.33 |
| mixed `get`/`getOrUpdate`/`put`/`modify`/`remove` | 7.78 ± 0.10 | 7.20 ± 0.20 | 5.78 ± 0.14 |

`foldMap` walks the whole cache, so it is measured per traversal of 10000 entries rather than per
key: 1148 ± 51, 1109 ± 44 and 989 ± 46 traversals per second respectively.

### Comparing against another revision

The module builds against the `scache` sources next to it, so an older revision is measured by
putting the module on top of that revision and running it there. Both runs have to happen on the
same machine, one after the other, or the numbers are not comparable.

```shell
git worktree add /tmp/scache-old <revision>
cp -r benchmark /tmp/scache-old/
cp build.sbt /tmp/scache-old/build.sbt
cp project/plugins.sbt /tmp/scache-old/project/plugins.sbt

cd /tmp/scache-old && sbt "benchmark/Jmh/run -rf json -rff /tmp/old.json"
cd -                && sbt "benchmark/Jmh/run -rf json -rff /tmp/new.json"
```

The `benchmark` project and the JMH plugin come from `build.sbt` and `project/plugins.sbt`, which is
why those two are copied over as well. If the older revision has a different internal API, the
benchmark will not compile there until the affected lines are adjusted. Going back past the `MapRef`
rewrite, for instance, only the `single` flavor needs it:

```scala
case "single" => LoadingCache.of(LoadingCache.EntryRefs.empty[IO, Int, Int])
```

Finally, `git worktree remove /tmp/scache-old` when done.

## Migrating to 7.0

The cache state moved from a single `Ref[F, Map[K, EntryRef]]` to a per-key `MapRef` over a
`ConcurrentHashMap`. What that means for the users:

**Type classes.** `Cache.loading`, `Cache.expiring`, `SerialMap.of` and `SerialMap.apply` now ask
for `Async[F]` instead of `Concurrent[F]` / `Temporal[F]`, because the new state needs `Sync` for
the `ConcurrentHashMap` next to `Concurrent` for the fibers. Nothing to do for `IO` or for any stack
that already has an `Async` instance, otherwise the call sites have to provide one.

**Removed.** `LoadingCache.EntryRefs`, and with it the overload
`LoadingCache.of(map: EntryRefs[F, K, V])`. Both were `private[scache]`, so this only affects code
inside this library. `LoadingCache.of[F, K, V]` replaces them.

**No more contention failures.** `getOrUpdate` used to give up with
`IllegalStateException("extreme contention")` after 10000 lost CAS attempts on the shared state.
Operations on distinct keys no longer contend at all, so the limit is gone along with that failure
mode.

**Cancelling a load cleans up.** Cancelling `getOrUpdate` now removes the entry it installed and
fails everyone waiting for that entry with `CancelledError`, instead of leaving the key unusable and
its waiters blocked forever. Code that cancels loads and expects the waiters to keep waiting has to
be adjusted.

**Loads can expire.** `ExpiringCache` evicts entries that have been loading longer than
`Config.loadingTimeout`, failing their waiters with `ExpiredError`. The load itself is not
cancelled, only detached from the cache. `loadingTimeout` defaults to the smaller of
`expireAfterRead` and `expireAfterWrite`, so set it explicitly if the loads are legitimately slower
than the expiration.

**Enumeration is weakly consistent.** `keys`, `values`, `values1`, `size`, `foldMap` and
`foldMapPar` are served by the `ConcurrentHashMap` and no longer observe an atomic snapshot of the
map: an entry added or removed concurrently may or may not be included.

## Release process
The release process is based on Git tags and makes use of [evolution-gaming/scala-github-actions](https://github.com/evolution-gaming/scala-github-actions) which uses [sbt-dynver](https://github.com/sbt/sbt-dynver) to automatically obtain the version from the latest Git tag. The flow is defined in `.github/workflows/release.yml`.  
A typical release process is as follows:
1. Create and push a new Git tag. The version should be in the format `vX.Y.Z` (example: `v4.1.0`). Example: `git tag v4.1.0 && git push origin v4.1.0`
2. On success, a new GitHub release is automatically created with a calculated diff and auto-generated release notes. You can see it on `Releases` page, change the description if needed
3. On failure, the tag is deleted from the remote repository. Please note that your local tag isn't deleted, so if the failure is recoverable then you can delete the local tag and try again (an example of *unrecoverable* failure is successfully publishing only a few of the artifacts to Artifactory which means a new attempt would fail since Artifactory doesn't allow overwriting its contents)
