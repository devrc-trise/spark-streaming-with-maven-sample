package org.rc.samples

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object SparkStreaming {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkStreamingSample")
    val ssc = new StreamingContext(conf, Seconds(15))
    val lines = ssc.textFileStream("hdfs://sandbox.hortonworks.com:8020/tmp/sparkstreaming-input")
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(w => (w, 1))
    val wordcounts = pairs.reduceByKey(_ + _)

    wordcounts.print()
    wordcounts.saveAsTextFiles("hdfs://sandbox.hortonworks.com:8020/tmp/sparkstreaming-output/wordcount")

    ssc.start()
    ssc.awaitTermination()
  }
}