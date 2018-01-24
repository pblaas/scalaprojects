
package pixelsoftware

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext, Time }
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrameWriter
import org.elasticsearch.spark.rdd.EsSpark

import org.elasticsearch.spark._
import org.elasticsearch.spark.streaming
import org.elasticsearch.spark.streaming.EsSparkStreaming
import org.elasticsearch.spark.rdd.EsSpark

import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.elasticsearch.spark.streaming._

import scala.collection.mutable.Queue

import java.util.regex.Pattern
import java.util.regex.Matcher

import org.apache.spark.streaming.flume._

import java.sql.Timestamp
import com.github.nscala_time.time.Imports._

/** Example of connecting to Flume log data, in a "pull" configuration. */
object Writetest {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("QueueStream")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", "127.0.0.1:9200")
    sparkConf.set("es.mapping.date.rich", "true")

    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    val rddQueue = new Queue[RDD[Trip]]()

    case class Trip(date: String, departure: String, arrival: String)
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val upcomingTrip = Trip(timestamp.toString(), "OTP", "SFO")
    val lastWeekTrip = Trip(timestamp.toString(), "MUC", "OTP")
    // Create the QueueInputDStream and use it do some processing
    val inputStream = ssc.queueStream(rddQueue)
    inputStream.print()
    //val dstream = ssc.queueStream(reducedStream)
    EsSparkStreaming.saveToEs(inputStream, "spark/docs")
    //EsSparkStreaming.saveToEs(inputStream, "spark/docs")

    ssc.start()

    // Create and push some RDDs into rddQueue
    for (i <- 1 to 10) {
      rddQueue.synchronized {
        rddQueue += ssc.sparkContext.makeRDD(Seq(upcomingTrip, lastWeekTrip))

      }
      Thread.sleep(1000)

    }
    ssc.stop()
  }
}
