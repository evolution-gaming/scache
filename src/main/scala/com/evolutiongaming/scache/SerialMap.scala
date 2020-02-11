package com.evolutiongaming.scache

import cats.Applicative
import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.effect.implicits._
import cats.implicits._
import com.evolutiongaming.catshelper.{Runtime, SerialRef}


/**
  * Map-like data structure, which runs updates serially for the same key
  */
trait SerialMap[F[_], K, V] {

  def get(key: K): F[Option[V]]

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

object SerialMap { self =>

  def empty[F[_] : Applicative, K, V]: SerialMap[F, K, V] = new SerialMap[F, K, V] {

    def get(key: K) = none[V].pure[F]

    def put(key: K, value: V) = none[V].pure[F]

    def modify[A](key: K)(f: Option[V] => F[(Option[V], A)]) = f(none[V]).map { case (_, value) => value }

    def update[A](key: K)(f: Option[V] => F[Option[V]]) = f(none[V]).void

    def size = 0.pure[F]

    def keys = Set.empty[K].pure[F]

    def values = Map.empty[K, V].pure[F]

    def remove(key: K) = none[V].pure[F]

    def clear = ().pure[F]
  }


  def apply[F[_]](implicit F: Concurrent[F]): Apply[F] = new Apply(F)


  def of[F[_] : Concurrent : Runtime, K, V]: F[SerialMap[F, K, V]] = of(None)


  def of[F[_] : Concurrent : Runtime, K, V](partitions: Int): F[SerialMap[F, K, V]] = of(Some(partitions))


  def of[F[_] : Concurrent : Runtime, K, V](partitions: Option[Int] = None): F[SerialMap[F, K, V]] = {
    for {
      cache <- Cache.loading[F, K, SerialRef[F, State[V]]](partitions).allocated
    } yield {
      apply(cache._1)
    }
  }


  def apply[F[_] : Concurrent, K, V](cache: Cache[F, K, SerialRef[F, State[V]]]): SerialMap[F, K, V] = {

    new SerialMap[F, K, V] { self =>

      def get(key: K) = {
        for {
          serialRef <- cache.get(key)
          state     <- serialRef.fold(State.empty[V].pure[F])(_.get)
        } yield state match {
          case State.Full(value) => value.some
          case State.Empty       => none[V]
          case State.Removed     => none[V]
        }
      }


      def put(key: K, value: V) = {
        modify(key) { prev =>
          (value.some, prev).pure[F]
        }
      }


      def modify[A](key: K)(f: Option[V] => F[(Option[V], A)]) = {

        def remove = cache.remove(key)

        def adding(added: Ref[F, Boolean]) = {
          for {
            _         <- added.set(true)
            serialRef <- SerialRef[F].of(State.empty[V])
          } yield serialRef
        }

        def modify(serialRef: SerialRef[F, State[V]], added: Ref[F, Boolean]) = {

          def modify(state: State[V]) = {

            def onValue(value: Option[V]) = {
              f(value).attempt.map {
                case Right((Some(value), a)) =>
                  val state = State.full(value)
                  val fa = a.pure[F]
                  (state, fa)

                case Right((None, a)) =>
                  val state = State.removed
                  val fa = remove.as(a)
                  (state, fa)

                case Left(error) =>
                  val fa = for {
                    added <- added.get
                    _     <- if (added) remove.void else ().pure[F]
                    a     <- error.raiseError[F, A]
                  } yield a
                  (state, fa)
              }
            }

            def onRemoving = {
              val state = State.removed[V]
              val fa = self.modify(key)(f)
              (state, fa).pure[F]
            }

            state match {
              case State.Full(value) => onValue(value.some)
              case State.Empty       => onValue(none)
              case State.Removed     => onRemoving
            }
          }

          for {
            a <- serialRef.modify(modify)
            a <- a
          } yield a
        }

        for {
          added     <- Ref[F].of(false)
          serialRef <- cache.getOrUpdate(key) { adding(added) }
          a         <- modify(serialRef, added).uncancelable
        } yield a
      }


      def update[A](key: K)(f: Option[V] => F[Option[V]]) = {
        modify(key) { value =>
          for {d <- f(value)} yield { (d, ()) }
        }
      }


      val size = cache.size


      val keys = cache.keys


      val values = {
        for {
          map  <- cache.values
          list <- map.foldLeft(List.empty[(K, V)].pure[F]) { case (values, (key, serialRef)) =>
            for {
              serialRef <- serialRef
              value     <- serialRef.get
              values    <- values
            } yield {
              value match {
                case State.Full(value) => (key, value) :: values
                case State.Empty       => values
                case State.Removed     => values
              }
            }
          }
        } yield {
          list.toMap
        }
      }


      def remove(key: K) = {
        modify(key) { value =>
          (none[V], value).pure[F]
        }
      }


      val clear = cache.clear.flatten
    }
  }


  class Apply[F[_]](val F: Concurrent[F]) extends AnyVal {

    def of[K, V](implicit runtime: Runtime[F]): F[SerialMap[F, K, V]] = self.of[F, K, V](F, runtime)
  }


  sealed trait State[+A]

  object State {

    def full[A](a: A): State[A] = Full(a)

    def removed[A]: State[A] = Removed

    def empty[A]: State[A] = Empty


    final case class Full[A](value: A) extends State[A]

    case object Empty extends State[Nothing]

    case object Removed extends State[Nothing]
  }
}
