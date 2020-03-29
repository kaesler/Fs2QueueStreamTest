package org.kae.fs2queuestreamtest

import cats.data.OptionT
import cats.effect.concurrent.Ref
import cats.effect.{ContextShift, Fiber, IO, Timer}
import cats.implicits._
import fs2.concurrent.Queue
import java.time.{Instant, Duration => JDuration}
import org.kae.fs2queuestreamtest.ticks.TickGenerators
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Random

class TestCase (
  val arrivalsPerSecond: Int,
  val pullBatchSize: Int,
  val pullPeriodSeconds: Int
) {
  final case class QueueElement(s: String)

  private val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)
  implicit val timer: Timer[IO] = IO.timer(ec)

  private val now: IO[Instant] = IO(Instant.now)

  private val lastArrivalTimeRef: Ref[IO, Option[Instant]] = Ref.unsafe[IO, Option[Instant]](None)
  private val recordArrival: IO[Unit] = now.map(Option.apply) >>= lastArrivalTimeRef.set

  private val lastPullTimeRef: Ref[IO, Option[Instant]] = Ref.unsafe[IO, Option[Instant]](None)
  private val lastPullSizeRef: Ref[IO, Option[Int]] = Ref.unsafe[IO, Option[Int]](None)
  private def recordPull(size: Int): IO[Unit] =
    for {
      time <- now
      _ <- lastPullTimeRef.set(Some(time))
      _ <- lastPullSizeRef.set(Some(size))
    } yield ()

  private val freshElt: IO[QueueElement] = IO(QueueElement(Random.nextString(24)))

  private val maximumTolerableQueueDelay = JDuration.ofMinutes(5)

  def lastArrivalTime: IO[Option[Instant]] = lastArrivalTimeRef.get
  def lastPullTime: IO[Option[Instant]] = lastPullTimeRef.get
  def lastPullSize: IO[Option[Int]] = lastPullSizeRef.get

  def showState: IO[String] =
    for {
      optArrivalTime <- lastArrivalTime
      optPullTime <- lastPullTime
      optPullSize <- lastPullSize
    } yield
      s"last arrival: ${optArrivalTime.map(_.toString).getOrElse("None")}\n" ++
      s"last pull:    ${optPullTime.map(_.toString).getOrElse("None")}\n" ++
      s"pull size:    ${optPullSize.map(_.toString).getOrElse("None")}\n"

  def start: IO[Unit] =
    for {
      queue <- Queue.unbounded[IO, QueueElement]
      _ <- startConsumer(queue)
      _ <- startProducer(queue)
    } yield ()

  def isBroken: IO[Boolean] =
    (for {
      lastArrival <- OptionT(lastArrivalTimeRef.get)
      lastPull <- OptionT(lastPullTimeRef.get)
      delay = JDuration.between(lastArrival, lastPull)
    } yield delay.toNanos > maximumTolerableQueueDelay.toNanos)
      .value
      .map(_.getOrElse(false))

  private def startProducer(queue: Queue[IO, QueueElement]): IO[Fiber[IO, Unit]] =
    TickGenerators.poissonArrivals(arrivalsPerSecond)
      .evalMap(_ => (freshElt >>= queue.offer1) <* recordArrival)
      .compile
      .drain
      .start

  private def startConsumer(queue: Queue[IO, QueueElement]): IO[Fiber[IO, Unit]] =
    queue.dequeue
      .groupWithin(pullBatchSize, pullPeriodSeconds.seconds)
      .evalTap { chunk =>
        // IO(println(s"pulled ${chunk.size}")) *>
        recordPull(chunk.size)
      }
      .map(_.toArray)
      .through(_.flatMap(consumeBatch))
      .compile
      .drain
      .start

  private def consumeBatch(elts: Array[QueueElement]): fs2.Stream[IO, Unit] =
    fs2.Stream.eval(batchServiceTime >>= IO.sleep)
      .drop(1)

  private val batchServiceTime: IO[FiniteDuration] =
    IO(Random.nextInt(30))
      .map(jitter =>(jitter + 10).milliseconds)
}
