package com.joseestudillo.spark_scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.joseestudillo.spark_scala.utils.SparkUtils
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.DataTypes

object UDFSparkSQL {
  val log = Logger(LoggerFactory.getLogger(getClass.getName));

  val JSON_TABLE_FILENAME = "table.json";

  val TABLE_NAME = "json_table";
	val FIELD_VALUE = "value";
	val FIELD_ID = "id";

	val UDF_NAME = "strLen";
  
  def main(args: Array[String]) {
    val host = if (args.length > 0) args(0) else SparkUtils.LOCAL_MASTER_ID
    val appName = getClass.getName

    val conf = new SparkConf().setAppName(appName).setMaster(host)
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)

    val jsonDataFrame = sqlContext.read.json(SparkUtils.getClasspathFileURI(JSON_TABLE_FILENAME));
		jsonDataFrame.registerTempTable(TABLE_NAME); //this gives a name to the table making it accessible

		// #DataFrame operations
		log.info("Show the whole table");
		jsonDataFrame.show();

		log.info(String.format("Declaring the UDF: %s", UDF_NAME));
		val udf: UDF1[String, Integer] = new UDF1[String, Integer]() {
		  override def call(s: String) = s.length()
		}
		
		sqlContext.udf.register(UDF_NAME, udf, DataTypes.IntegerType);

		var query = String.format("SELECT %1$s, %2$s(%1$s) FROM %3$s", FIELD_VALUE, UDF_NAME, TABLE_NAME);
		val udfAppliedDataFrame = jsonDataFrame.sqlContext.sql(query);
		log.info(String.format("Using the UDF %s in the query: %s", UDF_NAME, query));
		udfAppliedDataFrame.show();   
    
    sparkContext.stop();
  }
}