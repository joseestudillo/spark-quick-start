package com.joseestudillo.spark_scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.joseestudillo.spark_scala.utils.SparkUtils
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.Logger
import com.joseestudillo.spark.utils.DerbyManager
import java.sql.Connection
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import scala.collection.mutable
import scala.collection.mutable.HashMap
import org.apache.spark.sql.DataFrame

object JDBCSparkSQL {

  val log = Logger(LoggerFactory.getLogger(getClass.getName))

  val CREATE_TMP_TABLE_QUERY = "CREATE TEMPORARY TABLE jdbc_table USING org.apache.spark.sql.jdbc OPTIONS ( url \"%s\", dbtable \"%s\")"
	val DATABASE_NAME = "spark_database"
	val TABLE_NAME = DerbyManager.DERBY_TABLE_NAME

	val SPARK_JDBC = "jdbc"
	val SPARK_JDBC_OPT_URL = "url"
	val SPARK_JDBC_OPT_DBTABLE = "dbtable"
  
  def main(args: Array[String]) {
    val host = if (args.length > 0) args(0) else SparkUtils.LOCAL_MASTER_ID
    val appName = getClass.getName

    val conf = new SparkConf().setAppName(appName).setMaster(host)
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)

    //derby database instantiation and table creation
    val connStr = DerbyManager.getConnectionString(DATABASE_NAME)
		DerbyManager.loadDriver()
		val conn = DerbyManager.getDerbyConnection(DATABASE_NAME)
		DerbyManager.createDummyTable(conn)

		//show the content of the just created table
		log.info(String.format("Content in the table '%s':", TABLE_NAME))

    val options = mutable.HashMap.empty[String, String]
		options.put(SPARK_JDBC_OPT_URL, connStr)
		options.put(SPARK_JDBC_OPT_DBTABLE, TABLE_NAME)
		log.info(s"Loading table $TABLE_NAME from $connStr with the options: $options")
		val jdbcDataFrame = sqlContext.read.format(SPARK_JDBC).options(options.toMap).load()
		
		jdbcDataFrame.show()
		
    sparkContext.stop()
  }
}