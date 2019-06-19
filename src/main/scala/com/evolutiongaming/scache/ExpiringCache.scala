package com.evolutiongaming.scache

import cats.effect.concurrent.Ref
import cats.effect.{Clock, Concurrent, Resource, Timer}
import cats.implicits._
import cats.{Applicative, Monad}
import com.evolutiongaming.catshelper.ClockHelper._

import scala.concurrent.duration._

object ExpiringCache {

  private[scache] def of[F[_] : Concurrent : Timer, K, V](
    expireAfter: FiniteDuration,
    maxSize: Option[Int] = None
  ): Resource[F, Cache[F, K, V]] = {

    val cooldown      = (expireAfter.toMillis * 0.2).toLong
    val expireAfterMs = expireAfter.toMillis + (cooldown * 0.5).toLong
    val sleep         = Timer[F].sleep((expireAfterMs * 0.1).millis)

     def background(ref: Ref[F, LoadingCache.EntryRefs[F, K, Entry[V]]]) = {

       def removeExpired(key: K, entryRefs: LoadingCache.EntryRefs[F, K, Entry[V]]) = {

         def removeExpired(entry: Entry[V]) = {
           for {
             now    <- Clock[F].millis
             result <- if (entry.timestamp + expireAfterMs < now) ref.update { _ - key } else ().pure[F]
           } yield result
         }

         val entryRef = entryRefs.get(key)
         entryRef.foldMapM { entryRef =>
           for {
             entry  <- entryRef.get
             result <- entry match {
               case entry: LoadingCache.Entry.Loaded[F, Entry[V]]  => removeExpired(entry.value)
               case _    : LoadingCache.Entry.Loading[F, Entry[V]] => ().pure[F]
             }
           } yield result
         }
       }

       def notExceedMaxSize(maxSize: Int) = {

         def drop(entryRefs: LoadingCache.EntryRefs[F, K, Entry[V]]) = {

           case class Elem(key: K, timestamp: Long)

           val zero = List.empty[Elem]
           val entries = entryRefs.foldLeft(zero.pure[F]) { case (result, (key, entryRef)) => 
             for {
               result <- result
               entry  <- entryRef.get
             } yield entry match {
               case entry: LoadingCache.Entry.Loaded[F, Entry[V]]  => Elem(key, entry.value.timestamp) :: result
               case _    : LoadingCache.Entry.Loading[F, Entry[V]] => result
             }
           }

           for {
             entries <- entries
             drop     = entries.sortBy(_.timestamp).take(maxSize / 10)
             result  <- drop.foldMapM { elem => ref.update { _ - elem.key } }
           } yield result
         }

         for {
           entryRefs <- ref.get
           result    <- if (entryRefs.size > maxSize) drop(entryRefs) else ().pure[F]
         } yield result
       }
       
       val fa = for {
         _         <- sleep
         entryRefs <- ref.get
         result    <- entryRefs.keys.toList.foldMapM { key => removeExpired(key, entryRefs) }
          _        <- maxSize.foldMapM(notExceedMaxSize)
       } yield result

       fa.foreverM[Unit]
     }

     val result = for {
       ref   <- Ref[F].of(LoadingCache.EntryRefs.empty[F, K, Entry[V]])
       cache  = LoadingCache(ref)
       fiber <- Concurrent[F].start { background(ref) }
     } yield {
       val release = fiber.cancel
       val result = apply(ref, cache, cooldown)
       (result, release)
     }
     Resource(result)
   }


   def apply[F[_] : Monad : Clock, K, V](
     ref: Ref[F, LoadingCache.EntryRefs[F, K, Entry[V]]],
     cache: Cache[F, K, Entry[V]],
     cooldown: Long,
   ): Cache[F, K, V] = {

     implicit val monoidUnit = Applicative.monoid[F, Unit]

     def touch(key: K, entry: Entry[V]) = {

       def touch(timestamp: Long): F[Unit] = {

         def touch(entryRef: LoadingCache.EntryRef[F, Entry[V]]) = {
           entryRef.update {
             case entry: LoadingCache.Entry.Loaded[F, Entry[V]]  => LoadingCache.Entry.loaded(entry.value.touch(timestamp))
             case entry: LoadingCache.Entry.Loading[F, Entry[V]] => entry
           }
         }

         for {
           map    <- ref.get
           result <- map.get(key).foldMap(touch)
         } yield result
       }

       def shouldTouch(now: Long) = (entry.timestamp + cooldown) <= now

       /*TODO randomize cooldown to avoid contention?*/
       for {
         now    <- Clock[F].millis
         result <- if (shouldTouch(now)) touch(now) else ().pure[F]
       } yield result
     }

     new Cache[F, K, V] {

       def get(key: K) = {
         for {
           entry <- cache.get(key)
           _     <- entry.foldMap { entry => touch(key, entry) }
         } yield for {
           entry <- entry
         } yield {
           entry.value
         }
       }

       def getOrUpdate(key: K)(value: => F[V]) = {

         def entry = {
           for {
             value     <- value
             timestamp <- Clock[F].millis
           } yield {
             Entry(value, timestamp)
           }
         }

         for {
           entry <- cache.getOrUpdate(key)(entry)
           _     <- touch(key, entry)
         } yield {
           entry.value
         }
       }

       def put(key: K, value: V) = {
         for {
           timestamp <- Clock[F].millis
           entry      = Entry(value, timestamp)
           entry     <- cache.put(key, entry)
         } yield for {
           entry <- entry
         } yield for {
           entry <- entry
         } yield {
           entry.value
         }
       }

       def keys = cache.keys

       def values = {
         for {
           values <- cache.values
         } yield {
           values.mapValues { e =>
             for {
               e <- e
             } yield {
               e.value
             }
           }
         }
       }

       def remove(key: K) = {
         for {
           entry <- cache.remove(key)
         } yield for {
           entry <- entry
         } yield for {
           entry <- entry
         } yield {
           entry.value
         }
       }

       def clear = cache.clear
     }
   }


  final case class Entry[A](value: A, timestamp: Long) { self =>

    def touch(timestamp: Long): Entry[A] = {
      if (timestamp > self.timestamp) copy(timestamp = timestamp) else self
    }
  }
}