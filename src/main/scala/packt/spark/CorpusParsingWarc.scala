package spark.webcorpus

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import packt.HelperScala.{delimiterWarcWet, parseRawWarcRecord}
import packt.WarcRecord
import packt.spark.CorpusParsingWet

/**
 * Code for parsing .warc files of the WARC corpus
 *
 * @author Phil, https://github.com/g1thubhub
 */
object CorpusParsingWarc {

  def main(args: Array[String]) = {

    val threads = 2
    val session = SparkSession.builder.
      master(s"local[$threads]")
      .appName("Corpus Parsing")
      .getOrCreate()

    import session.implicits._
    import org.apache.spark.sql._

    val hadoopConf = session.sparkContext.hadoopConfiguration
    hadoopConf.set("textinputformat.record.delimiter", delimiterWarcWet)

    val inputLocationWarc = CorpusParsingWet.getClass.getResource("/spark/webcorpus/warc.sample").getPath
    val webpagesRDD: RDD[WarcRecord] = session.sparkContext
      .newAPIHadoopFile(inputLocationWarc, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hadoopConf)
      .flatMap(parseRawWarcRecord(_))
      .filter(_.warcType == "response")

    val webpagesDataset: Dataset[WarcRecord] = webpagesRDD.toDS()
    webpagesDataset.printSchema()
    /*
    root
    |-- warcType: string (nullable = true)
    |-- date: date (nullable = true)
    |-- recordID: string (nullable = true)
    |-- contentLength: integer (nullable = false)
    |-- contentType: string (nullable = true)
    |-- infoID: string (nullable = true)
    |-- concurrentTo: string (nullable = true)
    |-- ip: string (nullable = true)
    |-- targetURI: string (nullable = true)
    |-- payloadDigest: string (nullable = true)
    |-- blockDigest: string (nullable = true)
    |-- payloadType: string (nullable = true)
    |-- htmlContentType: string (nullable = true)
    |-- language: string (nullable = true)
    |-- htmlLength: integer (nullable = false)
    |-- htmlSource: string (nullable = true)
    */


    webpagesDataset.show(3)
    /*
    +--------+----------+--------------------+-------------+--------------------+--------------------+--------------------+--------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------+----------+--------------------+
    |warcType|      date|            recordID|contentLength|         contentType|              infoID|        concurrentTo|            ip|           targetURI|       payloadDigest|         blockDigest|         payloadType|     htmlContentType|language|htmlLength|          htmlSource|
    +--------+----------+--------------------+-------------+--------------------+--------------------+--------------------+--------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------+----------+--------------------+
    |response|1970-01-19|<urn:uuid:dc550ee...|        44287|application/http;...|<urn:uuid:47046f6...|<urn:uuid:c26c8cc...|104.27.160.112|http://013info.rs...|sha1:P5LGYLYIECUM...|sha1:AFZZNJ5YSPXI...|           text/html|text/html; charse...|      sr|     43365|<!DOCTYPE html PU...|
    |response|1970-01-19|<urn:uuid:e6068d3...|          652|application/http;...|<urn:uuid:47046f6...|<urn:uuid:53bd2b4...|203.107.32.173|http://016.kouyu1...|sha1:6R4DUYQ7DRZS...|sha1:T4G5RWLKOGI2...|application/xhtml...|text/html;charset...|    null|       287|<!DOCTYPE html PU...|
    |response|1970-01-19|<urn:uuid:b4a806a...|        13394|application/http;...|<urn:uuid:47046f6...|<urn:uuid:9408588...|47.100.201.254|http://01gydc.com...|sha1:IK4EFX2V7UB5...|sha1:D7R5CNGVF5MD...|           text/html|text/html;charset...|    null|     13048|<!DOCTYPE html>
    */


    // small subset of overall English records which were explicitly marked in server response, see sratchpad.txt
    val englishRecords = webpagesDataset.filter($"language" === "en")
    println(englishRecords.map(_.htmlSource).first())
    /*
    <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML+RDFa 1.0//EN"
      "http://www.w3.org/MarkUp/DTD/xhtml-rdfa-1.dtd">
      <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" version="XHTML+RDFa 1.0" dir="ltr">
      <head profile="http://www.w3.org/1999/xhtml/vocab">
      <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        ........
     */

  }

}
