package com.jai.spark.core;

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.SparkContext
import org.springframework.util.StopWatch

/**
 * Counts words in new text files created in the given directory
 * Usage: HdfsWordCount <directory>
 *   <directory> is the directory that Spark Streaming will use to find and read new text files.
 *
 * To run this on your local machine on directory `localdir`, run this example
 *    $ bin/run-example \
 *       org.apache.spark.examples.streaming.HdfsWordCount localdir
 *
 * Then create a text file in `localdir` and the words in the file will get counted.
 */
object SparkWordCount {

  def main(args: Array[String]) {

    val fn = if (args.length < 1)
      "./data/100krows.csv"
    else
      args(0)

    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local[*]")
    // Create the context
    val ssc = new SparkContext(sparkConf)

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val sw = new StopWatch("wordcount")
    val lines = ssc.textFile(fn)
    val words = lines.flatMap(_.split(","))
    val wordCounts = words.map(x => (x, 1))
    //wordCounts.foreach(println)
    val reduced= wordCounts.reduceByKey(_ + _)
    sw.stop()
    reduced.foreach(println)
    sw.prettyPrint()
    //val collected = wordCounts.collect()
    println(reduced.count)
    
  }

}
	// scalastyle:on println

