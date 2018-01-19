import Dependencies._

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

lazy val root = (project in file(".")).settings(
  inThisBuild(
    List(
      organization := "nl.pixelsoftware",
      scalaVersion := "2.11.11",
      version := "0.1.0-SNAPSHOT"
    )),
  name := "Stream",
  libraryDependencies ++= Seq(
    scalaTest % Test,
    "org.apache.spark" %% "spark-core" % "2.2.1" % "provided",
    "org.apache.spark" %% "spark-streaming" % "2.2.1" % "provided",
    "org.apache.spark" %% "spark-streaming-flume" % "2.2.1",
    "org.apache.spark" %% "spark-sql" % "2.2.1" % "provided",
    "org.apache.flume" % "flume-ng-sdk" % "1.8.0",
    "org.apache.flume" % "flume-ng-core" % "1.8.0"
  )
)

