package com.joseestudillo.spark.sql;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.joseestudillo.spark.utils.SparkUtils;

/**
 * Example of caching tables and dataframes in Spark
 * 
 * @author Jose Estudillo
 *
 */
public class MemoryManagementSparkSQL {

	public static final Logger log = Logger.getLogger(MemoryManagementSparkSQL.class);

	public static final String TABLE_NAME = "json_table";

	public static void main(String[] args) {
		SparkConf conf = SparkUtils.getLocalConfig(MemoryManagementSparkSQL.class.getSimpleName());
		log.info(String.format("access to the web interface at localhost: %s", SparkUtils.SPARK_UI_PORT));
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(sparkContext);

		DataFrame jsonDataFrame = sqlContext.read().json(SparkUtils.getClasspathFileURI("table.json"));
		jsonDataFrame.registerTempTable(TABLE_NAME); //this gives a name to the table making it accessible

		//for dataFrames, the caching is done as in any other RDD
		log.info("Caching DataFrame");
		jsonDataFrame.cache();

		// Caching a table is also configured using configuration properties in Spark. For example:
		//sqlContext.setConf("spark.sql.inMemoryColumnarStorage.compressed", "false");
		//sqlContext.setConf("spark.sql.inMemoryColumnarStorage.batchSize", "1000");

		log.info("Caching Table");
		sqlContext.cacheTable(TABLE_NAME);

		//do intensive operations with the table

		log.info("Uncaching Table");
		sqlContext.uncacheTable(TABLE_NAME);
	}
}
