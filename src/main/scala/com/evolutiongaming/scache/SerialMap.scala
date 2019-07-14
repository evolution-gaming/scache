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
  import SerialMap._

  def get(key: K): F[Option[V]]

  def put(key: K, value: V): F[Option[V]]

  def modify[A](key: K)(f: Option[V] => F[(Directive[V], A)]): F[A]

  def update[A](key: K)(f: Option[V] => F[Directive[V]]): F[Unit]

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

    def modify[A](key: K)(f: Option[V] => F[(Directive[V], A)]) = f(none[V]).map(_._2)

    def update[A](key: K)(f: Option[V] => F[Directive[V]]) = f(none[V]).void

    def size = 0.pure[F]

    def keys = Set.empty[K].pure[F]

    def values = Map.empty[K, V].pure[F]

    def remove(key: K) = none[V].pure[F]

    def clear = ().pure[F]
  }


  def apply[F[_]](implicit F: Concurrent[F]): Apply[F] = new Apply(F)


  def of[F[_] : Concurrent : Runtime, K, V]: F[SerialMap[F, K, V]] = {
    for {
      cache <- Cache.loading[F, K, SerialRef[F, State[V]]]()
    } yield {
      apply(cache)
    }
  }


  def apply[F[_] : Concurrent, K, V](cache: Cache[F, K, SerialRef[F, State[V]]]): SerialMap[F, K, V] = {

    new SerialMap[F, K, V] { self =>

      def get(key: K) = {
        for {
          serialRef <- cache.get(key)
          value     <- serialRef.fold(State.present(none[V]).pure[F]) { serialRef => serialRef.get }
        } yield value match {
          case State.Present(value) => value
          case State.Absent         => none[V]
        }
      }


      def put(key: K, value: V) = {
        modify(key) { prev =>
          val directive = Directive.update(value)
          (directive, prev).pure[F]
        }
      }


      def modify[A](key: K)(f: Option[V] => F[(Directive[V], A)]) = {

        def remove = cache.remove(key)

        def create(created: Ref[F, Boolean]) = {
          for {
            _         <- created.set(true)
            serialRef <- SerialRef[F].of(State.present(none[V]))
          } yield serialRef
        }

        def modify(serialRef: SerialRef[F, State[V]], created: Ref[F, Boolean]) = {

          def modify(state: State[V]) = {
            state match {
              case State.Absent     =>
                val state = State.absent[V]
                val fa = self.modify(key)(f)
                (state, fa).pure[F]

              case State.Present(v) =>
                f(v).attempt.map {
                  case Right((directive, a)) =>
                    directive match {
                      case Directive.Update(value) =>
                        val state = State.present(value.some)
                        val fa = a.pure[F]
                        (state, fa)

                      case Directive.Remove        =>
                        val state = State.absent
                        val fa = remove.as(a)
                        (state, fa)
                    }

                  case Left(error) =>
                    val fa = for {
                      created <- created.get
                      _       <- if (created) remove.void else ().pure[F]
                      a <- error.raiseError[F, A]
                    } yield a
                    (state, fa)
                }
            }
          }

          for {
            a <- serialRef.modify(modify)
            a <- a
          } yield a
        }

        for {
          created   <- Ref[F].of(false)
          serialRef <- cache.getOrUpdate(key) { create(created) }
          a         <- modify(serialRef, created).uncancelable
        } yield a
      }


      def update[A](key: K)(f: Option[V] => F[Directive[V]]) = {
        modify(key) { value =>
          for {d <- f(value)} yield { (d, ()) }
        }
      }


      val size = cache.size


      val keys = cache.keys


      val values = {
        for {
          map  <- cache.values
          list <- map.foldLeft(List.empty[(K, V)].pure[F]) { case (result, (key, serialRef)) =>
            for {
              serialRef <- serialRef
              value     <- serialRef.get
              result    <- result
            } yield {
              value match {
                case State.Present(Some(value)) => (key, value) :: result
                case State.Present(None)        => result
                case State.Absent               => result
              }
            }
          }
        } yield {
          list.toMap
        }
      }


      def remove(key: K) = {
        modify(key) { value =>
          val directive = Directive.remove[V]
          (directive, value).pure[F]
        }
      }


      val clear = cache.clear
    }
  }


  class Apply[F[_]](val F: Concurrent[F]) extends AnyVal {

    def of[K, V](implicit runtime: Runtime[F]): F[SerialMap[F, K, V]] = self.of[F, K, V](F, runtime)
  }


  sealed trait Directive[+A]

  object Directive {

    def update[A](a: A): Directive[A] = Update(a)

    def remove[A]: Directive[A] = Remove


    final case class Update[A](a: A) extends Directive[A]

    case object Remove extends Directive[Nothing]
  }


  sealed trait State[+A]

  object State {

    def absent[A]: State[A] = Absent

    def present[A](a: Option[A]): State[A] = Present(a)


    case object Absent extends State[Nothing]

    final case class Present[A](value: Option[A]) extends State[A]
  }
}
