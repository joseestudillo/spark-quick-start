package com.joseestudillo.spark_scala.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.broadcast.Broadcast
import com.joseestudillo.spark_scala.utils.SparkUtils

object BroadcastVariables {
  val log = Logger(LoggerFactory.getLogger(getClass.getName));

  def main(args: Array[String]) {
    val host = if (args.length > 0) args(0) else SparkUtils.LOCAL_MASTER_ID
    val appName = getClass.getName

    val conf = new SparkConf().setAppName(appName).setMaster(host)
    val sc = new SparkContext(conf)

    val integers = List.empty ++ (1 to 10)
    //creation
    val broadcastedIntList: Broadcast[List[Int]] = sc.broadcast(integers);

    //access
    broadcastedIntList.value;

    sc.stop();
  }
}