package com.joseestudillo.spark_scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory
import com.joseestudillo.spark_scala.utils.SparkUtils
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.Encoder
import scala.reflect.ClassTag
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes

object SparkSQL {
  
  val JSON_TABLE_FILENAME = "table.json"
	val TABLE_NAME = "json_table"
	val FIELD_VALUE = "value"
	val FIELD_ID = "id"
  
  val log = Logger(LoggerFactory.getLogger(getClass.getName))
  
  def main(args: Array[String]) {
    val host = if (args.length > 0) args(0) else SparkUtils.LOCAL_MASTER_ID
    val appName = SparkSQL.getClass.getName
   
    val conf = new SparkConf().setAppName(appName).setMaster(host)
    val sparkContext = new SparkContext(conf)
    val sparkSQLContext = new SQLContext(sparkContext)
    
    //import the implicits to allow converting collections and RDDs into dataframes.
    import sparkSQLContext.implicits._
    
    val numbers = List() ++ (0 to 10)
    val strings = numbers.map { x => "s" + x }
    val func: ((String, Int)) => String = (t) => s"${t._1}, ${t._2}"
    val tuples = strings.zip(numbers)
    val csv = tuples.map(func)
    
    val jsonFilePath = SparkUtils.getClasspathFileURI(JSON_TABLE_FILENAME)
    log.info(s"loading the file $jsonFilePath")
    val jsonDataFrame = sparkSQLContext.read.json(jsonFilePath)
    jsonDataFrame.registerTempTable(TABLE_NAME)

    log.info("Show table schema info")
    jsonDataFrame.printSchema()
    
    log.info("Show table content")
    jsonDataFrame.show()
    
		log.info("Use as resultSet")
		val rows = jsonDataFrame.select(FIELD_VALUE).collectAsList()
		for (row <- rows.toArray()) {
			log.info(String.valueOf(row))
		}

		log.info(s"select $FIELD_VALUE, $FIELD_ID +1")
		jsonDataFrame.select(jsonDataFrame.col(FIELD_VALUE), jsonDataFrame.col(FIELD_ID).plus(1)).show()

		log.info(s"filter by $FIELD_ID > 0")
		jsonDataFrame.filter(jsonDataFrame.col(FIELD_ID).gt(0)).show()

		log.info(s"group by $FIELD_VALUE")
		jsonDataFrame.groupBy(FIELD_VALUE).count().show()

		log.info(s"group by $FIELD_ID")
		jsonDataFrame.groupBy(FIELD_ID).count().show()

		val query = s"SELECT * FROM $TABLE_NAME"
		val sqlDataFrame = jsonDataFrame.sqlContext.sql(query)
		log.info(s"Using query: $query")
		sqlDataFrame.show()

		println(csv)
		
    sparkContext.stop()
  }
}