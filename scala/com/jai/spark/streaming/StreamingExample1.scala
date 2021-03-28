package com.jai.spark.streaming;

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }

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
object StreamingExample1 {

  def main(args: Array[String]) {

    val fn = if (args.length < 1)
      "./data/big.txt"
    else
      args(0)

    val sparkConf = new SparkConf().setAppName("HdfsWordCount")
    sparkConf.setMaster("local")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream(fn)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    
    ssc.awaitTermination()
  }

}
	// scalastyle:on println

