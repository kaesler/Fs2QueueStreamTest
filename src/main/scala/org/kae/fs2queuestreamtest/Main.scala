package org.kae.fs2queuestreamtest

import cats.effect.{CancelToken, ExitCode, Fiber, IO, IOApp}
import cats.implicits._
import java.time.Instant
import scala.concurrent.duration._

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    for {
      start <- now
      _ <- IO(println(s"started at $start"))
      tc <- new TestCase(5, 500, 1).pure[IO]
      pair <- tc.start
//      (producer, consumer) = pair
//      _ = scheduleCancellation(1.minute, consumer)
      _ <- check(tc)
      end <- now
      _ <- IO(println(s"ended at $end"))
    } yield ExitCode.Success
  }

  private def scheduleCancellation(
    delay: FiniteDuration,
    fiber: Fiber[IO, Unit]
  ): Unit = {
    val cancelToken: CancelToken[IO] = fiber.cancel
    (IO.sleep(delay) *> cancelToken).unsafeRunAsyncAndForget()
  }

  private def check(tc: TestCase): IO[Unit]  = {
    for {
      _ <- IO.sleep(15.seconds)
      checkTime <- now
      isBroken <- tc.isBroken
      _ <-
        if (isBroken)
          IO(println(s"Broken at $checkTime")) *> printState(tc)
        else
          IO(println(s"Okay at $checkTime")) *> printState(tc) *> check(tc)
    } yield ()
  }

  private def printState(tc: TestCase) =
    tc.showState >>= (text => IO(println(text)))

  private val now = IO(Instant.now)
}
