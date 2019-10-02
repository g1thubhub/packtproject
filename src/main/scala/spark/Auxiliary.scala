package spark

import org.apache.hadoop.io.{LongWritable, Text}

import scala.collection.mutable
import scala.util.matching.Regex

/**
  * Constants and helper functions
  *
  * @author Phil, https://github.com/g1thubhub
  */
object Auxiliary {

  val delimiterWarcWet = "WARC/1.0" // Wrong => Exception in thread "main" org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 0.0 failed 1 times, most recent failure: Lost task 0.0 in stage 0.0 (TID 0, localhost, executor driver): java.lang.OutOfMemoryError: Java heap space
  val delimiterWarcWetBytes: Array[Byte] = delimiterWarcWet.getBytes()
  val blankLine: Regex = "(?m:^(?=[\r\n]))".r
  val newLine = "[\\n\\r]+"

  // helper function for extracting meta info
  def extractWetMetaInfo(rawMetaInfo: String) = {
    val metaEntries = mutable.Map.empty[String, String]
    val fields = rawMetaInfo.split(newLine) // split string on newlines
    for(field <- fields) {
      val keyValue = field.split(":")
      metaEntries(keyValue(0).trim) = keyValue.slice(1, keyValue.length).mkString(":").trim
    }
    metaEntries
  }

  // parses raw WarcWet records into domain objects of type spark.WarcRecord
  def parseRawWetRecord(keyValue: (LongWritable, Text)): Option[WarcWetRecord] = {
    val rawContent = keyValue._2.toString // key is a line number which is is useless
    val matches = blankLine.findAllMatchIn(rawContent)
    if(matches.isEmpty) { // malformed record, skip
      None
    }
    else {
      val matchStarts =  matches.map(_.end).toList // get end points of matches, only first two elements are relevant
      val docStart = matchStarts(0) // start of record
      val boundary = matchStarts(1) // end of meta section
      val rawMetaInfo = rawContent.substring(docStart, boundary).trim
      val metaPairs = extractWetMetaInfo(rawMetaInfo)
      val pageContent = rawContent.substring(boundary + 1).trim
      Some(WarcWetRecord(metaPairs, pageContent))
    }
  }



  // helper function for extracting meta info
  def extractWarcMetaInfo(rawMetaInfo: String):  mutable.Map[String, String] = {
    val metaEntries = mutable.Map.empty[String, String]
    val fields = rawMetaInfo.split(newLine) // split string on newlines
    for(field <- fields) {
      val keyValue = field.split(":")
      metaEntries(keyValue(0).trim) = keyValue.slice(1, keyValue.length).mkString(":").trim
    }
    metaEntries
  }

  /*
  HTTP/1.1 200 OK
Date: Sat, 17 Aug 2019 20:32:22 GMT
Content-Type: text/html; charset=utf-8
X-Crawler-Transfer-Encoding: chunked
Connection: keep-alive
Set-Cookie: __cfduid=d18ff9b33e70487b9b45f0d6d8f5aae1f1566073941; expires=Sun, 16-Aug-20 20:32:21 GMT; path=/; domain=.013info.rs; HttpOnly
X-Drupal-Cache: MISS
Expires: Sun, 19 Nov 1978 05:00:00 GMT
Cache-Control: public, max-age=300
X-Content-Type-Options: nosniff
Content-Language: sr
X-Frame-Options: SAMEORIGIN
X-Generator: Drupal 7 (http://drupal.org)
Link: <https://013info.rs/sites/default/files/images/crna_hronika.jpg>; rel="image_src",<http://013info.rs/vesti/hronika/pancevo-ubistvo-na-sodari>; rel="canonical",<https://013info.rs/node/12357>; rel="shortlink"
Last-Modified: Sat, 17 Aug 2019 20:33:32 GMT
Vary: Cookie,Accept-Encoding
Server: cloudflare
CF-RAY: 507e6ab6dd22cf20-IAD
X-Crawler-Content-Encoding: gzip
Content-Length: 43365





   */
  def extractResponseMetaInfo(responseMeta: String): (String, Option[String], Int) = {
    val metaEntries = mutable.Map.empty[String, String]
    val fields = responseMeta.split(newLine) // split string on newlines
    var contentType, language = ""
    var contentLength = -1

    for(field <- fields) {
      if(field.startsWith("Content-Type:")) {
        contentType = field.substring(14).trim
      }
      else if(field.startsWith("Content-Language:")) {
        language = field.substring(17).trim

      }
      else if(field.startsWith("Content-Length:")) {
        contentLength = field.substring(15).trim.toInt
      }
    }
    (contentType, if(language.isEmpty) None else Some(language), contentLength)
  }

  // parses raw WarcWet records into domain objects of type spark.WarcRecord
  def parseRawWarcRecord(keyValue: (LongWritable, Text)): Option[WarcRecord] = {
    val rawContent = keyValue._2.toString // key is a line number which is is useless
    val matches = blankLine.findAllMatchIn(rawContent)
    if(matches.isEmpty) { // malformed record, skip
      None
    }
    else {
      val matchStarts =  matches.map(_.end).toList // get end points of matches, only first two elements are relevant
      val docStart = matchStarts(0) // start of record
      val metaBoundary = matchStarts(1) // end of meta section
      val serverBoundary = matchStarts(2) // end of server meta section
      val rawMetaInfo = rawContent.substring(docStart, metaBoundary).trim
      val metaPairs = extractWarcMetaInfo(rawMetaInfo)
      val responseMeta = rawContent.substring(metaBoundary + 1, serverBoundary).trim
      val responseMetaTriple = extractResponseMetaInfo(responseMeta)
      val pageContent = rawContent.substring(serverBoundary + 1).trim
      Some(WarcRecord(metaPairs, responseMetaTriple, pageContent))
    }
  }

}