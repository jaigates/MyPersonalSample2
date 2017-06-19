package com.jai.spark.core;

import java.nio.file._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory
import org.springframework.util.StopWatch
import org.apache.commons.io.FileUtils

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

  val log = LoggerFactory.getLogger("SparkWordCount")

  def main(args: Array[String]) {

    val fn = if (args.length < 1)
      "./data/majestic_million.csv"
    else
      args(0)

    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local[*]")
    // Create the context
    val ssc = new SparkContext(sparkConf)

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val sw = new StopWatch(fn)
    sw.start("textfile")
    val lines = ssc.textFile(fn)
    sw.stop()
    sw.start("flatMap")
    val words = lines.flatMap(_.split(","))
    sw.stop()
    sw.start("map")
    val wordCounts = words.map(x => (x, 1))
    sw.stop()
    //wordCounts.foreach(log.info)
    sw.start("reduceByKey")
    val reduced = wordCounts.reduceByKey(_ + _)
    sw.stop()
    //sw.start("priting foreach")
    //reduced.map(x=>log.info(s"$x._1 : $x._2"))
    //reduced.foreach(x=>log.info(s"$x"))
    
    val out = Paths.get("./output/sparkwordcount").toFile() 
    FileUtils.forceDelete(out);
    sw.start("saveAsTextFile")
    reduced.saveAsTextFile(out.getCanonicalPath)
    sw.stop()
    log.info(sw.prettyPrint())
    //val collected = wordCounts.collect()
    log.info("" + reduced.count)

  }

}
	// scalastyle:on log.info

