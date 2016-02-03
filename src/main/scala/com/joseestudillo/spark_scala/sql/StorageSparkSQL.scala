package com.joseestudillo.spark_scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.joseestudillo.spark_scala.utils.SparkUtils
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SQLContext
import java.io.File
import org.apache.commons.io.FileUtils

object StorageSparkSQL {
  val log = Logger(LoggerFactory.getLogger(getClass.getName))

  val TABLE_NAME = "json_table"
	val FIELD_VALUE = "value"
	val FIELD_KEY = "key"

	val SPARK_PARQUET_FORMAT = "parquet"

	val JSON_TABLE_FILENAME = "table.json"
	val PARQUET_QUERY_FILENAME = "/tmp/query.parquet"
	val PARQUET_SCHEMA_FILENAME = "/tmp/schema.parquet"
  
  def main(args: Array[String]) {
    val host = if (args.length > 0) args(0) else SparkUtils.LOCAL_MASTER_ID
    val appName = getClass.getName

    val conf = new SparkConf().setAppName(appName).setMaster(host)
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)


    val jsonFilePath = SparkUtils.getClasspathFileURI(JSON_TABLE_FILENAME)
		log.info(String.format("Loading data from %s", jsonFilePath))
		val jsonDataFrame = sqlContext.read.json(jsonFilePath)
		jsonDataFrame.registerTempTable(TABLE_NAME) //this gives a name to the table making it accessible

		val queryParquetFile = new File(PARQUET_QUERY_FILENAME)
		val schemaParquetFile = new File(PARQUET_SCHEMA_FILENAME)
		for (file <- List(queryParquetFile, schemaParquetFile)) {
			if (file.exists()) {
				FileUtils.deleteDirectory(file)
			}
		}

		//Saving the result of a query
		log.info(String.format("Storing DataFrame query into %s using parquet", queryParquetFile))
		jsonDataFrame.select(FIELD_VALUE).write.format(SPARK_PARQUET_FORMAT).save(queryParquetFile.getPath())

		//Loading the result of a query from a parquet file
		log.info(String.format("Loading data from parquet file: %s", queryParquetFile))
		val loadedParquetFileQueryDataFrame = sqlContext.read.parquet(queryParquetFile.getPath())
		log.info("Loaded DataFrame:")
		loadedParquetFileQueryDataFrame.show()

		//Saving a whole dataframe
		log.info(String.format("Storing a complete DataFrame into %s using parquet", schemaParquetFile))
		jsonDataFrame.write.parquet(schemaParquetFile.getPath())

		//Loading a whole dataframe from a parquet file
		log.info(String.format("Loading %s into a DataFrame", schemaParquetFile))
		val loadedParquetFileDataFrame = sqlContext.read.parquet(schemaParquetFile.getPath())
		log.info("Loaded DataFrame:")
		loadedParquetFileDataFrame.show()
    
    sparkContext.stop()
  }
}