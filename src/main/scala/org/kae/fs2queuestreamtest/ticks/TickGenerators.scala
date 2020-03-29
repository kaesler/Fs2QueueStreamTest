package org.kae.fs2queuestreamtest.ticks

import breeze.stats.distributions.Exponential
import cats.effect.{IO, Timer}
import fs2.{Pure, Stream}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
  * Streams of timed events.
  */
object TickGenerators {

  private implicit lazy val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  /**
    * A continuous [[Stream]] of ticks.
    */
  val continuous: Stream[IO, Unit] = Stream.duration[IO].map(_ => ())

  /**
    * @return A [[Stream]] of ticks at a specified rate.
    * @param ticksPerSecond number of ticks per second.
    */
  def fixedRate(ticksPerSecond: Int): Stream[IO, Unit] =
    Stream.fixedRate((1000000.00 / ticksPerSecond).toLong.microsecond)

  /**
    * @return a [[Stream]] of events distributed according to a Poisson
    * distribution at a specified average rate.
    * @param ticksPerSecond the average rate
    */
  def poissonArrivals(ticksPerSecond: Int): Stream[IO, Unit] = {

    val randomExponentiallyDistributedNumbers: Iterator[Double] =
      new Iterator[Double] {
        private val exp = new Exponential(ticksPerSecond.toDouble)

        override val hasNext: Boolean = true
        override def next(): Double = exp.draw()
      }

    val timesBetweenArrivals: Stream[Pure, Double] =
      Stream.unfold(randomExponentiallyDistributedNumbers) { i =>
        Some((i.next(), i))
      }

    val ticks: Stream[IO, Unit] =
      timesBetweenArrivals
        .evalMap { secondsToNextArrival =>
          val durationToNextArrival =
            (secondsToNextArrival * 1000000).toLong.microseconds
          IO.sleep(durationToNextArrival)
        }
    ticks
  }
}
