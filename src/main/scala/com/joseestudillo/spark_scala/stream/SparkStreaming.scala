package com.joseestudillo.spark_scala.stream

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.joseestudillo.spark_scala.utils.SparkUtils
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.streaming.Durations

object SparkStreaming {
  
  val BASE_DURATION = 2

	val NC_PORT = 9999

	val BATCH_DURATION = Durations.seconds(BASE_DURATION)

  // window is the amount of time to check
	val WINDOW_DURATION = Durations.seconds(BASE_DURATION * 3)
  //the slide defines how often the results are computed
	val SLIDE_DURATION = Durations.seconds(BASE_DURATION * 1)

  val log = Logger(LoggerFactory.getLogger(getClass.getName))

  def main(args: Array[String]) {
    val host = if (args.length > 0) args(0) else SparkUtils.LOCAL_MASTER_ID
    val appName = getClass.getName

    val conf = new SparkConf().setAppName(appName).setMaster(host)
    val sparkContext = new SparkContext(conf)

    sparkContext.stop()
  }
}