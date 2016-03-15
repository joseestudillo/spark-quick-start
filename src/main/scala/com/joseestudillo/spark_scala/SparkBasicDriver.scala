package com.joseestudillo.spark_scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.Logger
import com.joseestudillo.spark_scala.utils.SparkUtils
import org.apache.spark.rdd.RDD

/**
 * Example of a basic driver in spark
 * 
 * @author Jose Estudillo
 */

object SparkBasicDriver {

  val log = Logger(LoggerFactory.getLogger(getClass.getName))

  def main(args: Array[String]) {
    val host = if (args.length > 0) args(0) else SparkUtils.LOCAL_MASTER_ID
    val appName = SparkBasicDriver.getClass.getName
   
    val conf = new SparkConf().setAppName(appName).setMaster(host)
    val sc = new SparkContext(conf)
    
    val integers = List.empty ++ (1 to 10)
    val intsRdd: RDD[Int] = sc.parallelize(integers)
    log.info(s"Input integers collection: ${intsRdd.collect()}")

    // # map
    val stringsRdd: RDD[String] = intsRdd.map(x => String.valueOf(x))
    log.info(s"map: Integer -> String: ${stringsRdd.collect().mkString(", ")}")

    // # reduction
    val intReduction = intsRdd.reduce((a, b) => a + b)
    val stringReduction = stringsRdd.reduce((a, b) => a + b)
    log.info(s"Integers ${intsRdd.collect().mkString(", ")} reduced using + to $intReduction")
    log.info(s"Strings ${stringsRdd.collect().mkString(", ")} reduced using + to $stringReduction")

    sc.stop()
  }
}
