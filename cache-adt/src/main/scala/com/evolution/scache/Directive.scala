package com.evolution.scache

sealed trait Directive[+F[_], +V]
object Directive {
  final case class Put[F[_], V](value: V, release: Option[F[Unit]]) extends Directive[F, V]
  case object Remove extends Directive[Nothing, Nothing]
  case object Ignore extends Directive[Nothing, Nothing]
}
