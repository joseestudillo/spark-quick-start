package com.joseestudillo.spark_scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory
import com.joseestudillo.spark_scala.utils.SparkUtils
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.catalyst.expressions.Expression

object HiveSparkSQL {
  val log = Logger(LoggerFactory.getLogger(getClass.getName))

  val HIVE_TABLE_NAME = "hive_table"
	val JSON_TABLE_NAME = "json_table"
	val HIVE_INPUT_FILE = "hive-input.txt"
	val JSON_TABLE_FILE = "table.json"
	val FIELD_KEY = "key"
	val FIELD_ID = "id"
	val FIELD_VALUE = "value"
  
  def main(args: Array[String]) {
    val host = if (args.length > 0) args(0) else SparkUtils.LOCAL_MASTER_ID
    val appName = getClass.getName

    val conf = new SparkConf().setAppName(appName).setMaster(host)
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)

    val hiveInputFilepath = SparkUtils.getClasspathFileURI(HIVE_INPUT_FILE)
		

		val hiveContext = new HiveContext(sparkContext)

		//hive table creation
		var hiveQuery = String.format("DROP TABLE IF EXISTS %s", HIVE_TABLE_NAME)
		log.info(String.format("Running query: %s", hiveQuery))
		hiveContext.sql(hiveQuery)

		hiveQuery = String.format("CREATE TABLE IF NOT EXISTS %s (%s INT, %s STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", HIVE_TABLE_NAME, FIELD_KEY,
				FIELD_VALUE)
		log.info(String.format("Running query: %s", hiveQuery))
		hiveContext.sql(hiveQuery)

		hiveQuery = String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE %s", hiveInputFilepath, HIVE_TABLE_NAME)
		log.info(String.format("Running query: %s", hiveQuery))
		hiveContext.sql(hiveQuery)

		hiveQuery = String.format("FROM %s SELECT key, value", HIVE_TABLE_NAME)
		log.info(String.format("Running query: %s", hiveQuery))
		val hiveQueryDataFrame = hiveContext.sql(hiveQuery)
		hiveQueryDataFrame.show()

		// Creating table from RDD / loading information into it
		//TODO find something smarter than dumping the content to a file and loading it hive style

		//create DataFrame from a file
		val jsonInputFilename = SparkUtils.getClasspathFileURI(JSON_TABLE_FILE)
		log.info(String.format("Loading the json file '%s' into a table:", jsonInputFilename))
		val jsonDataFrame = sqlContext.read.json(jsonInputFilename)
		jsonDataFrame.registerTempTable(JSON_TABLE_NAME)

		log.info(String.format("DataFrame for Table '%s' created from file %s:", JSON_TABLE_NAME, jsonInputFilename))
		jsonDataFrame.show()
		log.info(String.format("Table '%s' to join to", HIVE_TABLE_NAME))
		hiveQueryDataFrame.show()
		
		//join between dataframe and hive table
		val joinExpr = hiveQueryDataFrame.col(FIELD_KEY).equalTo(jsonDataFrame.col(FIELD_ID))
		val joinResultDataFrame = hiveQueryDataFrame.join(jsonDataFrame, joinExpr)
		log.info(String.format("Joined table: %s with %s having %s=%s:", HIVE_TABLE_NAME, JSON_TABLE_NAME, FIELD_KEY, FIELD_ID))
		joinResultDataFrame.show()
    
    sparkContext.stop()
  }
}