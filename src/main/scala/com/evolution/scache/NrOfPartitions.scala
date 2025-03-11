package com.evolution.scache

import cats.FlatMap
import cats.syntax.all.*
import com.evolutiongaming.catshelper.Runtime

object NrOfPartitions {

  def apply[F[_]: FlatMap: Runtime](): F[Int] = {
    Runtime[F].availableCores
      .map { _ + 1 }
  }
}
