package com.joseestudillo.spark_scala.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.joseestudillo.spark_scala.utils.SparkUtils
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.Logger

object Partitioning {
  val log = Logger(LoggerFactory.getLogger(getClass.getName));

  def main(args: Array[String]) {
    val host = if (args.length > 0) args(0) else SparkUtils.LOCAL_MASTER_ID
    val appName = getClass.getName

    val conf = new SparkConf().setAppName(appName).setMaster(host)
    val sc = new SparkContext(conf)

    //TODO
    
    sc.stop()
  }
}