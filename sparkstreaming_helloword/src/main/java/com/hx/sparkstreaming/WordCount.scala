package com.hx.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created  on 2019/03/26.
  *
  *
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setMaster("local[3]").setAppName("WordCount")
    val ssc = new StreamingContext(conf,Seconds(1))
    val lines = ssc.socketTextStream("hadoop101",9999)
    val words = lines.flatMap(_.split(" "))

    val pairs=words.map(word =>(word,1))
    val wordCounts=pairs.reduceByKey(_+_)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
