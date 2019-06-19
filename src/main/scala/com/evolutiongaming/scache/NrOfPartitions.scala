package com.evolutiongaming.scache

import cats.FlatMap
import cats.implicits._
import com.evolutiongaming.catshelper.Runtime


object NrOfPartitions {

  def apply[F[_] : FlatMap : Runtime](): F[Int] = {
    for {
      cpus <- Runtime[F].availableCores
    } yield {
      2 + cpus
    }
  }
}
