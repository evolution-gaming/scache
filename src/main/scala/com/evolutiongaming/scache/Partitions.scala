package com.evolutiongaming.scache

import cats.Monad
import cats.implicits._
import cats.kernel.Hash

trait Partitions[-K, +V] {

  def get(key: K): V

  def values: List[V]
}

object Partitions {

  type Partition = Int


  def const[K, V](value: V): Partitions[K, V] = new Partitions[K, V] {

    def get(key: K) = value

    val values = List(value)
  }

  def of[F[_] : Monad, K : Hash, V](
    nrOfPartitions: Int,
    valueOf: Partition => F[V]
  ): F[Partitions[K, V]] = {

    def apply(nrOfPartitions: Int) = {

      val partitions = (0 until nrOfPartitions).toList.foldLeftM(List.empty[V]) { (a, b) =>
        for {
          value <- valueOf(b)
        } yield {
          value :: a
        }
      }
      for {
        partitions <- partitions
      } yield {
        Partitions[K, V](partitions.reverse.toVector)
      }
    }

    if (nrOfPartitions <= 1) valueOf(0).map(const) else apply(nrOfPartitions)
  }


  private def apply[K : Hash, V](partitions: Vector[V]): Partitions[K, V] = {

    val nrOfPartitions = partitions.size

    new Partitions[K, V] {

      def get(key: K) = {
        val hash = key.hash
        val partition = math.abs(hash % nrOfPartitions)
        partitions(partition)
      }

      val values = partitions.toList
    }
  }
}