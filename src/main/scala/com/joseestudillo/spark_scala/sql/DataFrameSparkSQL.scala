package com.joseestudillo.spark_scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.joseestudillo.spark_scala.utils.SparkUtils
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row

object DataFrameSparkSQL {
  
  val log = Logger(LoggerFactory.getLogger(getClass.getName))
  
  val TABLE_NAME = "databean_table"
  
  def main(args: Array[String]) {
    
    class DataBean(var name: String,var id: Long) extends Serializable {
      override def toString = s"DataBean($name, $id)"
      def setName(s: String) { name = s }
      def setId(l: Long) { id = l }
      def getName = name
      def getId = id
    }
    
    val host = if (args.length > 0) args(0) else SparkUtils.LOCAL_MASTER_ID
    val appName = getClass.getName

    val conf = new SparkConf().setAppName(appName).setMaster(host)
    val sparkContext = new SparkContext(conf)
    val sparkSQLContext = new SQLContext(sparkContext)


    val inputList = List("abc, 1", "def, 2", "ghi, 3")
		val inputRdd: RDD[String] = sparkContext.parallelize(inputList)

		val toDataBean: String => DataBean = x => {
		  val tokens = x.split(", ") 
		  new DataBean(tokens(0), tokens(1).toLong)
		}
		
		// transform the plain text into DataBeans
		val dataBeanRDD: RDD[DataBean] = inputRdd.map(toDataBean)
		log.info(s"Transforming the input ${inputRdd.collect().mkString(", ")} into dataBeans ${dataBeanRDD.collect().mkString(", ")}")

		// Create a DataFrame from the DataBeans RDD
		val dataBeanDataFrame = sparkSQLContext.createDataFrame(dataBeanRDD, classOf[DataBean])
		dataBeanDataFrame.registerTempTable(TABLE_NAME)
		log.info(s"Table '$TABLE_NAME' generated from ${dataBeanRDD.collect().mkString(", ")}:")
		dataBeanDataFrame.show()

		// Get all the rows in the dataFrame
		val rddFromDataFrame: RDD[Row] = dataBeanDataFrame.rdd

		// log the rows in a JSON format
		val fromRowToString = (row: Row) => {
		  s"Row: {id:${row.getLong(0)}, name:${row.getString(1)}}"
		}
		log.info(s"Transforming rowRdd ${rddFromDataFrame.collect().mkString(", ")} into strings: ${rddFromDataFrame.map(fromRowToString).collect().mkString(", ")}")

		// # Read data into a dataframe using plain data and defining the schema
		// define the column names/types
		val columnTypes: Array[StructField] = new Array[StructField](2)
		columnTypes.update(0, DataTypes.createStructField("name", DataTypes.StringType, true))
		columnTypes.update(1, DataTypes.createStructField("id", DataTypes.LongType, true))
		val tableSchema: StructType = DataTypes.createStructType(columnTypes)

		log.info(s"Table Squema: $tableSchema")

		// Create rows (Row) from plain text
		val fromStringToRow = (inputText: String) => {
			val fields = inputText.split(",")
			Row(fields(0).trim(), fields(1).trim().toLong)
		}
		val rowsFromDataRDD: RDD[Row] = inputRdd.map(fromStringToRow)
		log.info(s"inputRdd ${inputRdd.collect().mkString(", ")} -> outputRowRdd ${rowsFromDataRDD.collect().mkString(", ")}")

		// Apply the schema to the RDD and load the Rows from the RDD.
		val definedSchemaDataFrame: DataFrame = sparkSQLContext.createDataFrame(rowsFromDataRDD, tableSchema)
		definedSchemaDataFrame.registerTempTable(TABLE_NAME)
		log.info(s"Table '$TABLE_NAME' generated from ${rowsFromDataRDD.collect().mkString(", ")}:")
		definedSchemaDataFrame.show()
    
    sparkContext.stop()
  }
}