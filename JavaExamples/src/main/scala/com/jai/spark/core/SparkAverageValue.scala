package com.jai.spark.core

import org.apache.spark._
import java.nio.file.Files
import java.nio.file.Paths
import org.apache.commons.io.FileUtils
import java.io.File
import org.springframework.util.StopWatch
import org.slf4j.LoggerFactory
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.math.NumberUtils

object SparkAverageValue {
  val log = LoggerFactory.getLogger("SparkAverageValue" )
  val sparkConf = new SparkConf(true).setMaster("local[*]")
    .setAppName("SparkAverageValue").set("spark.ui.port","5050")
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("INFO")

  val fn = "./data/price.csv"
  //val fn = "./data/10rows.csv"

  def method1(): Unit = {

    val data = sc.textFile(fn, 3)

    FileUtils.deleteDirectory(new File("./output/SparkAverageValue_method1"));
    val sw = new StopWatch("method1")
    sw.start
    val header = data.first()
    data.filter(_ != header)
      .map(_.split(","))
      .map(x => (x(0) + "-" + x(1) + "-" + x(2), (x(3).toFloat, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + 1))
      .map(x => (x._1, x._2._1 / x._2._2))
      .sortByKey()
      .coalesce(1)
      .saveAsTextFile("./output/SparkAverageValue_method1")
    sw.stop()
    log.warn(sw.prettyPrint())
  }
  
  
   def method2(): Unit = {
   
    val data = sc.textFile(fn, 3)

    FileUtils.deleteDirectory(new File("./output/SparkAverageValue_method2"));
    val sw = new StopWatch("method1")
    log.warn("begin map")
    sw.start
    //val header = data.first()
    //val data2 = data.map( x=> ( x.split(",")(0), x.split(",").filter( NumberUtils.isNumber(_) ) )).map ( x => (x._1,  x._2.map(_.toFloat)   ) )
    val data2 = data.map( x=> ( x.replaceAll(",","---"), x.split(",").filter( NumberUtils.isNumber(_) ) )).map ( x => (x._1,  x._2.map(_.toFloat)   ) )
    val data3 =   data2.reduceByKey ( (x,y) =>  {
      println(x+"###"+y)
      x ++ y 
    })
    //data3.coalesce(1)
    data3.saveAsTextFile("./output/SparkAverageValue_method2")
    /*
      .map(x => (x,x.split(",").filter( StringUtils.isNumeric(_)) ))
      .map(x => (x(0) + "-" + x(1) + "-" + x(2), (x(3).toFloat, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + 1))
      .map(x => (x._1, x._2._1 / x._2._2))
      .sortByKey()
      .coalesce(1)
      .saveAsTextFile("./output/SparkAverageValue_method1")
      */
    sw.stop()
    log.warn(sw.prettyPrint())
  }

  def main(args: Array[String]): Unit = {

    method2()
    //val data = sc.textFile("./data/price.txt",3)

    /* from spark streaming presentaiton 
    * data.map{ x=> (x(0),(x(1),1)) }
        .reduceByKey(case (x,y) => (x._1 + y._1, x._2 + y._2) )
        .map{ x => (x._1, x._2(0) / x._2(1)) }
        .collect*/

  }

  def generateKey(x: Array[String]) = {
    ""
  }

}