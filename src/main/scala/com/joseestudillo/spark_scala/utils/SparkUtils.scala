package com.joseestudillo.spark_scala.utils

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SparkUtils {
  
  val LOCAL_MASTER_ID = "local[*]";
  val DEFAULT_SEP = ", "
  
	def getLocalConfig(appName: String ): SparkConf = {
		new SparkConf().setAppName(appName).setMaster(LOCAL_MASTER_ID);
	}
	
	def toString(rdd: RDD[_], sep: String = DEFAULT_SEP): String = {
	  rdd.collect().mkString(sep)
	}
}