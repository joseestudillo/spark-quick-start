package com.joseestudillo.spark_scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.joseestudillo.spark_scala.utils.SparkUtils
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SQLContext

object MemoryManagementSparkSQL {
  
  val log = Logger(LoggerFactory.getLogger(getClass.getName))

  val TABLE_NAME = "json_table"
  val JSON_FILE = "table.json"

  def main(args: Array[String]) {
    val host = if (args.length > 0) args(0) else SparkUtils.LOCAL_MASTER_ID
    val appName = getClass.getName

    val conf = new SparkConf().setAppName(appName).setMaster(host)
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)


    val jsonDataFrame = sqlContext.read.json(SparkUtils.getClasspathFileURI(JSON_FILE))
		jsonDataFrame.registerTempTable(TABLE_NAME) //this gives a name to the table making it accessible

		//for dataFrames, the caching is done as in any other RDD
		log.info("Caching DataFrame")
		jsonDataFrame.cache()

		// Caching a table is also configured using configuration properties in Spark. For example:
		//sqlContext.setConf("spark.sql.inMemoryColumnarStorage.compressed", "false")
		//sqlContext.setConf("spark.sql.inMemoryColumnarStorage.batchSize", "1000")

		log.info("Caching Table")
		sqlContext.cacheTable(TABLE_NAME)

		//do intensive operations with the table

		log.info("Uncaching Table")
		sqlContext.uncacheTable(TABLE_NAME)
    
    sparkContext.stop()
  }
}