package spark

import java.time.Instant
import java.time.format.DateTimeParseException

import scala.collection.mutable

/**
  * Domain objects for the WARC data
  *
  * @author Phil, https://github.com/g1thubhub
  */
case class WarcWetRecord(warcType: String, targetURI: String, date: java.sql.Date, recordID: String, refersTo: String, digest: String, contentType: String, contentLength: Int, plainText: String)

object WarcWetRecord {
  def apply(metaPairs: mutable.Map[String, String], pageContent: String): WarcWetRecord = {
    val warcType = metaPairs.getOrElse("WARC-Type", "")
    val targetURI = metaPairs.getOrElse("WARC-Target-URI", "")
    val recordID = metaPairs.getOrElse("WARC-Record-ID", "")
    val refersTo = metaPairs.getOrElse("WARC-Refers-To", "")
    val digest = metaPairs.getOrElse("WARC-Block-Digest", "")
    val contentType = metaPairs.getOrElse("Content-Type", "")

    var contentLength = -1
    var dateMs = -1L
    try {
      contentLength = metaPairs.getOrElse("Content-Length", "-1").toInt
      dateMs = Instant.parse(metaPairs.getOrElse("WARC-Date", "+1000000000-12-31T23:59:59.999999999Z")).getEpochSecond
    }
    catch {
      case _: NumberFormatException => System.err.println(s"Malformed contentLength field for record $recordID")
      case _: DateTimeParseException => System.err.println(s"Malformed date field for record $recordID")
      case e: Exception => e.printStackTrace()
    }
    WarcWetRecord(warcType, targetURI, new java.sql.Date(dateMs), recordID, refersTo, digest, contentType, contentLength, pageContent)
  }
}

case class WarcRecord(warcType: String, date: java.sql.Date, recordID: String, contentLength: Int, contentType: String, infoID: String, concurrentTo: String, ip: String, targetURI: String, payloadDigest: String, blockDigest: String, payloadType: String, htmlContentType: String, language: Option[String], htmlLength: Int , htmlSource: String)

object WarcRecord {
  def apply(metaPairs: mutable.Map[String, String], responseMeta: (String, Option[String], Int), sourceHtml: String): WarcRecord = {
    val warcType = metaPairs.getOrElse("WARC-Type", "")
    var dateMs = -1L
    val recordID = metaPairs.getOrElse("WARC-Record-ID", "")
    var contentLength = -1
    val contentType = metaPairs.getOrElse("Content-Type", "")
    val infoID = metaPairs.getOrElse("WARC-Warcinfo-ID", "")
    val concurrentTo = metaPairs.getOrElse("WARC-Concurrent-To", "")
    val ip = metaPairs.getOrElse("WARC-IP-Address", "")
    val targetURI = metaPairs.getOrElse("WARC-Target-URI", "")
    val payloadDigest = metaPairs.getOrElse("WARC-Payload-Digest", "")
    val blockDigest = metaPairs.getOrElse("WARC-Block-Digest", "")
    val payloadType = metaPairs.getOrElse("WARC-Identified-Payload-Type", "")

    try {
      contentLength = metaPairs.getOrElse("Content-Length", "-1").toInt
      dateMs = Instant.parse(metaPairs.getOrElse("WARC-Date", "+1000000000-12-31T23:59:59.999999999Z")).getEpochSecond
    }
    catch {
      case _: NumberFormatException => System.err.println(s"Malformed contentLength field for record $recordID")
      case _: DateTimeParseException => System.err.println(s"Malformed date field for record $recordID")
      case e: Exception => e.printStackTrace()
    }
    WarcRecord(warcType, new java.sql.Date(dateMs), recordID, contentLength, contentType, infoID, concurrentTo, ip, targetURI, payloadDigest, blockDigest, payloadType, responseMeta._1, responseMeta._2, responseMeta._3, sourceHtml)
  }
}


/*
/*

: http://013info.rs/vesti/hronika/pancevo-ubistvo-na-sodari
: sha1:P5LGYLYIECUMABRETXYXG5FTATT44Y55
: sha1:AFZZNJ5YSPXI6HUNH3PGWKA7MHE3PWM2
: text/html

 */



 */




object Test extends App {


  //Date string with offset information
  val dateString = "2019-08-17T21:02:56Z"


  val parsed = Instant.parse(dateString)
  println(parsed)

  println(Instant.MAX)


}


//case class WarcInfoRecord(warcType: String, warcDate: String, warcRecordId: String, contentType: String, contentLength: Int, pageText: String, filename: String) extends spark.WarcRecord(warcType, warcDate, warcRecordId, contentType, contentLength, pageText)
//case class WarcInfoRecord(warcType: String, warcDate: String, warcRecordId: String, contentType: String, contentLength: Int, pageText: String, filename: String) extends spark.WarcRecord(warcType, warcDate, warcRecordId, contentType, contentLength, pageText)