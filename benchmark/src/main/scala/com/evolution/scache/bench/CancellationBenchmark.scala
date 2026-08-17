package com.evolution.scache.bench

import CancellationBenchmark.*
import cats.data.{NonEmptyList, NonEmptyMap}
import cats.effect.unsafe.implicits.global
import cats.effect.{ExitCode, FiberIO, IO, IOApp, Resource}
import cats.syntax.all.*
import com.evolution.scache
import com.evolution.scache.ExpiringCache
import com.evolutiongaming.catshelper.ParallelHelper.*
import org.openjdk.jmh.annotations.{
  Benchmark,
  BenchmarkMode,
  Fork,
  Level,
  Measurement,
  Mode,
  OutputTimeUnit,
  Scope,
  Setup,
  State,
  TearDown,
  Threads,
  Warmup,
}

import java.util.concurrent.TimeUnit
import scala.collection.immutable.SortedMap
import scala.concurrent.duration.*
import scala.util.Random
import scala.util.control.NoStackTrace

/**
 * To run benchmarks: {{{sbt benchmark/Jmh/run com.evolution.scache.bench.CancellationBenchmark}}}
 *
 * Results on Apple M3 Max using Oracle JDK 17.0.11
 *
 * {{{
 * ==original==
 * Benchmark                               Mode    Cnt    Score   Error  Units
 * CancellationBenchmark.cancelSingleShot    ss  10000  691.013 ± 5.735  us/op
 * }}}
 *
 * {{{
 * ==MapRef==
 * Benchmark                               Mode    Cnt    Score   Error  Units
 * CancellationBenchmark.cancelSingleShot    ss  10000  669.808 ± 6.001  us/op
 * }}}
 */
@Fork(value = 1, jvmArgsAppend = Array("-Xmx4g"))
@Threads(1)
class CancellationBenchmark {

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Warmup(iterations = 200, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 10000, timeUnit = TimeUnit.SECONDS)
  def cancelSingleShot(state: CancellationState): Unit = state.subject()

//  @Benchmark
//  @BenchmarkMode(Array(Mode.Throughput))
//  @OutputTimeUnit(TimeUnit.SECONDS)
//  @Warmup(iterations = 1, time = 20, timeUnit = TimeUnit.SECONDS)
//  @Measurement(iterations = 1, time = 180, timeUnit = TimeUnit.SECONDS)
//  def cancelThroughput(state: CancellationState): Unit = state.subject()
}

@State(Scope.Benchmark)
class CancellationState {

  private var release: IO[Unit] = IO.unit
  private var cancellationFiber: FiberIO[Unit] = null

  @Setup(Level.Invocation)
  def setupInvocation(): Unit = {
    val consumer: Resource[IO, ConsumerOf[IO]] = ConsumerOf.make
    val topicFlow: Resource[IO, TopicFlow] = TopicFlow.make(CacheOf())

    val (flow, release) = topicFlow.allocated.unsafeRunSync()
    this.release = release

    val load = consumer.use { _.poll.flatMap(flow.apply) }
    this.cancellationFiber = {
      val loading = for {
        loadingFiber <- load.start
        _ <- IO.sleep(10.milliseconds)
        //        _ <- loadingFiber.cancel
      } yield loadingFiber
      loading.unsafeRunSync()
    }
  }

  def subject(): Unit =
    cancellationFiber.cancel.unsafeRunSync()

  @TearDown(Level.Invocation)
  def tearDownInvocation(): Unit =
    release.unsafeRunSync()
}

object Test extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val consumer: Resource[IO, ConsumerOf[IO]] = ConsumerOf.make
    val topicFlow: Resource[IO, TopicFlow] = TopicFlow.make(CacheOf())

    for {
      _ <- IO.unit
      load = (consumer, topicFlow).tupled.use { case (consumer, topicFlow) =>
        consumer.poll.flatMap(topicFlow.apply)
          .timed
          .flatMap { case (duration, result) =>
            IO(println(s"poll done in ${ duration.toMicros }")).as(result)
          }
      }
        .timed
        .flatMap { case (duration, result) =>
          IO(println(s"all done in ${ duration.toMicros }")).as(result)
        }
      cancellation =
        for {
          loadingFiber <- load.start
          _ <- IO.sleep(10.milliseconds)
          _ <- loadingFiber.cancel
            .timed
            .flatMap { case (duration, result) =>
              IO(println(f"cancel done in ${ duration.toMicros }%08d μs")).as(result)
            }
        } yield ()
      _ <- cancellation.replicateA(3)
    } yield ExitCode.Success
  }
}

object CancellationBenchmark {
  type Partition = Int
  val error: Throwable = new RuntimeException("ba-bam!") with NoStackTrace
}

case class Record(key: String, value: Int)

object Poll {
  def build: NonEmptyMap[Partition, NonEmptyList[Record]] = {
    val numberOfKeys = 250 // Random.nextInt(250) + 1
    val numberOfRecords = 1000 // Random.nextInt(1000) + 1
    val numberOfPartitions = 16 // Random.nextInt(16) + 1

    val keys = (0 until numberOfKeys).map(i => f"key-$i%06d")
    val records = (0 until numberOfRecords).map(Record(keys(Random.nextInt(numberOfKeys)), _))

    NonEmptyMap.fromMapUnsafe {
      SortedMap.from {
        records.groupBy(_.value % numberOfPartitions).map { case (partition, records) =>
          partition -> NonEmptyList.fromListUnsafe(records.toList)
        }
      }
    }
  }
}

trait ConsumerOf[F[_]] {
  def poll: F[NonEmptyMap[Partition, NonEmptyList[Record]]]
}

object ConsumerOf {
  def make: Resource[IO, ConsumerOf[IO]] = {
    val consumerOf = new ConsumerOf[IO] {
      override def poll: IO[NonEmptyMap[Partition, NonEmptyList[Record]]] =
        IO.pure(Poll.build)
    }
    Resource.pure(consumerOf)
  }
}

trait CacheOf[F[_]] {
  def make[K, V]: Resource[F, Cache[F, K, V]]
}

trait Cache[F[_], K, V] {
  def getOrUpdate(key: K)(value: => Resource[F, V]): F[V]
}

object CacheOf {
  def apply(): CacheOf[IO] = {
    class Main
    new Main with CacheOf[IO] {
      def make[K, V]: Resource[IO, Cache[IO, K, V]] = {
        val config = ExpiringCache.Config[IO, K, V](expireAfterRead = 1.minute)
        for {
          cache <- scache.Cache.expiring(config)
        } yield {
          new Cache[IO, K, V] {
            def getOrUpdate(key: K)(value: => Resource[IO, V]): IO[V] = {
              cache.getOrUpdateResource(key) { value }
            }
          }
        }
      }
    }
  }
}

object ReplicateRecords {
  def process(record: Record): IO[NonEmptyList[Int]] =
    IO.sleep(50.milliseconds) *>
      //    if (Random.nextInt(chanceToFailOneIn) == 0) IO.raiseError(error)
      //    else IO(NonEmptyList.one(1))
      IO(NonEmptyList.one(1 + record.value - record.value))
}

trait TopicFlow {
  def apply(records: NonEmptyMap[Partition, NonEmptyList[Record]]): IO[Unit]
}

// fill the cache
object TopicFlow {
  trait PartitionFlow {
    def apply(records: NonEmptyList[Record]): IO[Unit]
  }

  trait KeyFlow {
    def apply(records: NonEmptyList[Record]): IO[Int]
  }

  def make(cacheOf: CacheOf[IO]): Resource[IO, TopicFlow] =
    cacheOf.make[Partition, PartitionFlow].map { partitionCache =>
      new TopicFlow {
        override def apply(records: NonEmptyMap[Partition, NonEmptyList[Record]]): IO[Unit] = {
          for {
            result <- records.parFoldMap1 {
              case (partition, records) =>
                //                val replicatePartition =
                for {
                  partitionFlow <- partitionCache.getOrUpdate(partition) {
                    for {
                      keyCache <- cacheOf.make[String, KeyFlow]
                    } yield new PartitionFlow {
                      override def apply(records: NonEmptyList[Record]): IO[Unit] = {
                        records
                          .groupBy { _.key }
                          .parFoldMap1 {
                            case (key, records) =>
                              //                                val replicateRecords =
                              for {
                                keyFlow <- keyCache.getOrUpdate(key) {
                                  Resource.pure {
                                    new KeyFlow {
                                      override def apply(records: NonEmptyList[Record]): IO[Int] = {
                                        records
                                          .flatTraverse(ReplicateRecords.process)
                                          .flatTap(_ => IO(println(s"$key: done")))
                                          .as(records.size)
                                      }
                                    }
                                  }
                                }
                                _ <- keyFlow(records)
                              } yield ()
                          }
                      }
                    }
                  }
                  //                  _ = println(s"partitionFlow: $partitionFlow ($partition, records: ${ records.size })")
                  result <- partitionFlow(records)
                } yield result
            }
          } yield result
        }
      }
    }
}
