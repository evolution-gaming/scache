package com.evolution.scache

import cats.Id
import cats.kernel.Hash
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PartitionsSpec extends AnyWordSpec with Matchers {

  "apply" should {

    implicit val hash: Hash[Int] = Hash.fromUniversalHashCode[Int]

    val partitions = Partitions.of[Id, Int, String](3, _.toString)

    "get" in {
      for {
        n <- 0 to 10
      } yield {
        partitions.get(n) shouldEqual (n % 3).toString
      }
    }

    "Partitions.values" in {
      partitions.values shouldEqual List("0", "1", "2")
    }

    "handle Int.MinValue hash correctly" in {
      // Int.MinValue is a special case: math.abs(Int.MinValue) == Int.MinValue (negative)
      // Our implementation uses (hash & Int.MaxValue) which always produces a non-negative value
      val intMinHash: Hash[Int] = new Hash[Int] {
        def hash(x: Int): Int = Int.MinValue
        def eqv(x: Int, y: Int): Boolean = x == y
      }
      val p = Partitions.of[Id, Int, String](3, _.toString)(implicitly, intMinHash)
      // Should not throw ArrayIndexOutOfBoundsException
      val result = p.get(42)
      result.toInt should be >= 0
      result.toInt should be < 3
    }

    "handle negative hash codes" in {
      val negativeHash: Hash[Int] = new Hash[Int] {
        def hash(x: Int): Int = -x
        def eqv(x: Int, y: Int): Boolean = x == y
      }
      val p = Partitions.of[Id, Int, String](4, _.toString)(implicitly, negativeHash)
      for {
        n <- 1 to 100
      } yield {
        val partition = p.get(n).toInt
        partition should be >= 0
        partition should be < 4
      }
    }

    "distribute keys reasonably across partitions" in {
      val nPartitions = 8
      val p = Partitions.of[Id, Int, String](nPartitions, _.toString)
      val counts = Array.fill(nPartitions)(0)
      for (n <- 0 until 10000) {
        counts(p.get(n).toInt) += 1
      }
      // Each partition should get at least 5% of keys (ideal is 12.5%)
      for (i <- 0 until nPartitions) {
        counts(i) should be > 500
      }
    }
  }


  "const" should {

    val partitions = Partitions.const[Int, String]("0")

    "get" in {
      for {
        n <- 0 to 10
      } yield {
        partitions.get(n) shouldEqual "0"
      }
    }

    "Partitions.values" in {
      partitions.values shouldEqual List("0")
    }
  }
}
