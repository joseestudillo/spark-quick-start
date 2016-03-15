package com.joseestudillo.spark_scala.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.Logger
import com.joseestudillo.spark_scala.utils.SparkUtils
import org.apache.spark.rdd.RDD

/**
 * Example of accumulators in spark.
 * 
 * @author Jose Estudillo
 */

object Accumulators {

  val log = Logger(LoggerFactory.getLogger(getClass.getName))

  def main(args: Array[String]) {
    val host = if (args.length > 0) args(0) else SparkUtils.LOCAL_MASTER_ID
    val appName = getClass.getName
   
    val conf = new SparkConf().setAppName(appName).setMaster(host)
    val sc = new SparkContext(conf)
    
    val integers = List.empty ++ (1 to 10)
    val intsRdd: RDD[Int] = sc.parallelize(integers)
    log.info("Input integers collection: %s".format(intsRdd.collect()))

    // # map
    val stringsRdd: RDD[String] = intsRdd.map(x => String.valueOf(x))
    log.info("map: Integer -> String: %s".format(stringsRdd.collect()))

    // # reduction
    val intReduction = intsRdd.reduce((a, b) => a + b)
    val stringReduction = stringsRdd.reduce((a, b) => a + b)
    log.info("Integers %s reduced using + to %s".format(intsRdd.collect().mkString(", "), intReduction))
    log.info("Strings %s reduced using + to %s".format(stringsRdd.collect().mkString(", "), stringReduction))

    sc.stop()
  }
}
