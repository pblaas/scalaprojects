
package pixelsoftware

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext, Time }
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.functions._

import org.elasticsearch.spark._
import org.elasticsearch.spark.streaming
import org.apache.spark.streaming.flume._

import java.util.regex.Pattern
import java.util.regex.Matcher

import com.github.nscala_time.time.Imports._

import Utilities._

/** Example of connecting to Flume log data, in a "pull" configuration. */
object Stream {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("BarracudaStream").set("spark.sql.warehouse.dir", "/tmp").set("spark.cores.max", "4")
    //conf.set("master", "local")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "elasticsearch-client.elktest.svc.cluster.local:9200")
    conf.set("es.nodes.wan.only", "false")
    conf.set("es.mapping.date.rich", "true")
    conf.set("es.ingest.pipeline", "geoip")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(10))

    setupLogging()

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = barracudaLogPattern()

    // The only difference from the push example is that we use createPollingStream instead of createStream.
    val flumeStream = FlumeUtils.createPollingStream(ssc, "xxx.xxx.nl", 9988)

    // This creates a DStream of SparkFlumeEvent objects. We need to extract the actual messages.
    // This assumes they are just strings, like lines in a log file.
    // In addition to the body, a SparkFlumeEvent has a schema and header you can get as well. So you
    // could handle structured data if you want.
    val lines = flumeStream.map(x => new String(x.event.getBody().array()))
    // Extract the request field from each log line

    val requests = lines.map(x => {
      val matcher: Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val month = matcher.group(1)
        val day = matcher.group(2).toInt
        val time = matcher.group(3)
        val device = matcher.group(4)
        val chain = matcher.group(5)
        val fqdn = matcher.group(6)
        val clientip = matcher.group(8)
        val messageid = matcher.group(9)
        val bytesin = matcher.group(10).toInt
        val bytesout = matcher.group(11).toInt
        val action = matcher.group(12)
        val sender = matcher.group(13)
        val receiver = matcher.group(14)
        val filteraction = matcher.group(15).toInt
        val filterreason = matcher.group(16).toInt
        val ip = matcher.group(17)
        (month, day, time, device, chain, fqdn, clientip, messageid, bytesin, bytesout, action, sender, receiver, filteraction, filterreason, ip)
      } else {
        (null, 0, null, null, null, null, null, null, 0, 0, null, null, null, 0, 0, null)
      }
    })
    //.window(Seconds(300))

    // Process each RDD from each batch as it comes in
    requests.foreachRDD((rdd, time) => {
      // So we'll demonstrate using SparkSQL in order to query each RDD
      // using SQL queries.

      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      //val inputStream = ssc.queueStream(rddQueue)
      import sqlContext.implicits._

      // SparkSQL can automatically create DataFrames from Scala "case classes".
      // We created the Record case class for this purpose.
      // So we'll convert each RDD of tuple data into an RDD of "Record"
      // objects, which in turn we can convert to a DataFrame using toDF()
      val requestsDataFrame = rdd.map(w => Record(w._1, w._2, w._3, w._4, w._5, w._6, w._7, w._8, w._9, w._10, w._11, w._12, w._13, w._14, w._15, w._16)).toDF()
      val requestsDataFramecount = requestsDataFrame.count()
      println(requestsDataFramecount)
      //requestsDataFrame.show(5)
      val cleandf = requestsDataFrame.na.drop()
      // Create a SQL table from this DataFrame
      cleandf.createOrReplaceTempView("requests")

      // But remember it's only querying the data in this RDD, from this batch.
      val wordCountsDataFrame =
        sqlContext.sql("select month, day, time, device, clientip, sender, receiver from requests")

      println(s"========= $time =========")
      val df = wordCountsDataFrame.groupBy("clientip").count().sort($"count".desc)
      val dfwithtime = df.withColumn("created", lit(System.currentTimeMillis()))
      val sparkIndex = s"spark-${{ java.time.LocalDate.now }}/docs"
      dfwithtime.write.format("org.elasticsearch.spark.sql").mode("append").save(sparkIndex)
      dfwithtime.show(5)
    })

    // Kick it off
    ssc.checkpoint("/tmp")
    ssc.start()
    ssc.awaitTermination()
  }
}

/** Case class for converting RDD to DataFrame */
case class Record(month: String, day: Int, time: String, device: String, chain: String, fqdn: String, clientip: String, messageid: String, bytesin: Int, bytesout: Int, action: String, sender: String, receiver: String, filteraction: Int, filterreason: Int, ip: String)

/**
 * Lazily instantiated singleton instance of SQLContext
 *  (Straight from included examples in Spark)
 */
object SQLContextSingleton {

  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
