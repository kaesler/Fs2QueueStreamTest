name := "Fs2QueueStreamTest"

version := "0.1"

scalaVersion := "2.13.1"

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "2.0.0",
  "org.typelevel" %% "cats-effect" % "2.0.0",
  "co.fs2" %% "fs2-core" % "2.2.2",
  "org.scalanlp" %% "breeze" % "1.0",
  "io.dropwizard.metrics" % "metrics-core" % "4.1.3"
)
