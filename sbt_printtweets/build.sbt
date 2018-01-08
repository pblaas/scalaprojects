name := "PrintTweets"

version := "1.0"

organization := "com.sundogsoftware"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "2.2.1" % "provided",
"org.apache.spark" %% "spark-streaming" % "2.2.1" % "provided",
"org.twitter4j" % "twitter4j-stream" % "4.0.6",
"org.twitter4j" % "twitter4j-core" % "4.0.6"
)

unmanagedJars in Compile += file("lib/dstream-twitter_2.11-0.1.0-SNAPSHOT.jar")
