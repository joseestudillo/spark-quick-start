package com.joseestudillo.spark.sql;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.joseestudillo.spark.SparkTextSearch;
import com.joseestudillo.spark.utils.SparkUtils;

public class MemoryManagementSparkSQL {

	public static final Logger log = Logger.getLogger(MemoryManagementSparkSQL.class);

	public static void main(String[] args) {
		SparkConf conf = SparkUtils.getLocalConfig(SparkTextSearch.class.getSimpleName());
		log.info("access to the web interface at localhost:4040");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(sparkContext);

		String tableName = "json_table";
		String field0 = "value";
		String field1 = "id";

		DataFrame jsonDataFrame = sqlContext.read().json(SparkUtils.getClasspathFileFullPath("table.json"));
		jsonDataFrame.registerTempTable(tableName); //this gives a name to the table making it accessible

		//for dataFrames, the caching is done as in any other RDD
		log.info("Caching DataFrame");
		jsonDataFrame.cache();

		// Caching a table is also configured using configuration properties in Spark that can be set as follows:
		//sqlContext.setConf("spark.sql.inMemoryColumnarStorage.compressed", "false");
		//sqlContext.setConf("spark.sql.inMemoryColumnarStorage.batchSize", "1000");

		log.info("Caching Table");
		sqlContext.cacheTable(tableName);

		//do intensive operations with the table

		log.info("Uncaching Table");
		sqlContext.uncacheTable(tableName);
	}
}
