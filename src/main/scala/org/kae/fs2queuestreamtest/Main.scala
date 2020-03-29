package org.kae.fs2queuestreamtest

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import java.time.Instant
import scala.concurrent.duration._

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    for {
      start <- now
      _ <- IO(println(s"started at $start"))
      tc <- new TestCase(400, 500, 1).pure[IO]
      _ <- tc.start
      _ <- check(tc)
      end <- now
      _ <- IO(println(s"ended at $end"))
    } yield ExitCode.Success
  }

  private def check(tc: TestCase): IO[Unit]  = {
    for {
      _ <- IO.sleep(5.minutes)
      checkTime <- now
      isBroken <- tc.isBroken
      _ <- if (isBroken)
        IO.unit
      else for {
        state <- tc.showState
        _ <- IO(println(s"Okay at $checkTime"))
        _ <- IO(println(state))
        _ <- check(tc)
      } yield ()
    } yield ()
  }

  private val now = IO(Instant.now)
}
