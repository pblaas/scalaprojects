package pixelsoftware

import org.apache.log4j.Level
import java.util.regex.Pattern
import java.util.regex.Matcher

object Utilities {

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{ Level, Logger }
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  /** Configures Twitter service credentials using twiter.txt in the main workspace directory */
  def setupTwitter() = {
    import scala.io.Source

    for (line <- Source.fromFile("twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }

  /** Retrieves a regex Pattern for parsing Apache access logs. */
  def apacheLogPattern(): Pattern = {
    val ddd = "\\d{1,3}"
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex =
      s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    Pattern.compile(regex)
  }

  def barracudaLogPattern(): Pattern = {
    val ddd = "\\d{1,3}"
    val month = "(\\S+)"
    val day = "(\\d{1,})"
    val time = "(\\d{2}:\\d{2}:\\d{2})"
    val device = "(\\S+)"
    val chain = "(\\S+)"
    val fqdn = "(\\S+)"
    val messageid = "(\\S+)"
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val clientip = s"(\\[$ip\\])"
    val bytesin = "(\\d{1,})"
    val bytesout = "(\\d{1,})"
    val action = "(\\S+)"
    val sender = "(\\S+)"
    val receiver = "(\\S+)"
    val filteraction = "(\\d{1,})"
    val filterreason = "(\\d{1,})"

    val regex2 =
      s"$month\\s+$day $time $device  $chain\\: $fqdn$clientip $messageid $bytesin $bytesout $action $sender $receiver $filteraction $filterreason $ip"
    Pattern.compile(regex2)
  }

}
