package com.joseestudillo.spark.sql;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import com.joseestudillo.spark.SparkTextSearch;
import com.joseestudillo.spark.utils.SparkUtils;

/**
 * 
 * @author Jose Estudillo
 *
 */
public class HiveSparkSQL {

	private static final Logger log = LogManager.getLogger(HiveSparkSQL.class);

	public static void main(String[] args) throws IOException {
		SparkConf conf = SparkUtils.getLocalConfig(SparkTextSearch.class.getSimpleName());
		log.info("access to the web interface at localhost:4040");
		JavaSparkContext spark = new JavaSparkContext(conf);

		String hiveInputFilepath = SparkUtils.getClasspathFileFullPath("hive-input.txt");
		String hiveTableName = "hive_table";
		String hiveQuery;

		HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(spark.sc());

		//TODO with none of these I'm able to set up the configuration programmatically, only placing hive-site.xml in the classpath works
		//		Properties properties = SparkUtils.loadProperties("/hive.properties");
		//		for (Object property : properties.keySet()) {
		//			String key = String.valueOf(property);
		//			String value = String.valueOf(properties.get(property));
		//			log.info(String.format("Setting (%s=%s)", key, value));
		//			hiveContext.hiveconf().set(key, value);
		//			hiveContext.conf().setConf(key, value);
		//			hiveContext.setConf(key, value);
		//		}

		hiveQuery = String.format("DROP TABLE IF EXISTS %s", hiveTableName);
		log.info(String.format("Running %s", hiveQuery));
		hiveContext.sql(hiveQuery);
		hiveQuery = String.format("CREATE TABLE IF NOT EXISTS %s (key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", hiveTableName);
		log.info(String.format("Running %s", hiveQuery));
		hiveContext.sql(hiveQuery);
		hiveQuery = String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE %s", hiveInputFilepath, hiveTableName);
		log.info(String.format("Running %s", hiveQuery));
		hiveContext.sql(hiveQuery);

		hiveQuery = String.format("FROM %s SELECT key, value", hiveTableName);
		log.info(String.format("Running %s", hiveQuery));
		DataFrame hiveQueryDataFrame = hiveContext.sql(hiveQuery);
		hiveQueryDataFrame.show();

		// Creating table from RDD / loading information into it
		//TODO find something smarter than dumping the content to a file and loading it hive style

		//join between hive and rdd
		String jsonTableName = "json_table";
		SQLContext sqlContext = new SQLContext(spark);
		String jsonInputFilename = SparkUtils.getClasspathFileFullPath("table.json");
		DataFrame jsonDataFrame = sqlContext.read().json(jsonInputFilename);
		jsonDataFrame.registerTempTable(jsonTableName);

		log.info(String.format("Table '%s' created from file %s:", jsonTableName, jsonInputFilename));
		jsonDataFrame.show();
		log.info(String.format("Table '%s' to join", hiveTableName));
		hiveQueryDataFrame.show();

		Column joinExpr = new Column(hiveQueryDataFrame.col("key").expr()).equalTo(jsonDataFrame.col("id"));
		DataFrame joinResultDataFrame = hiveQueryDataFrame.join(jsonDataFrame, joinExpr);
		log.info(String.format("Joined table: %s with %s having %s=%s:", hiveTableName, jsonTableName, "key", "id"));
		joinResultDataFrame.show();

		spark.close();
	}
}
