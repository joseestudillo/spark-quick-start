package com.joseestudillo.spark.sql;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import com.joseestudillo.spark.utils.LoggerUtils;
import com.joseestudillo.spark.utils.SparkUtils;

/**
 * Example of how to use Hive from Spark
 * 
 * TODO: I only have been able to load Hive Configuration placing hive-site.xml in the classpath.
 * 
 * @author Jose Estudillo
 */
public class HiveSparkSQL {

	private static final Logger log = Logger.getLogger(HiveSparkSQL.class);

	private static final String HIVE_TABLE_NAME = "hive_table";
	private static final String JSON_TABLE_NAME = "json_table";
	private static final String HIVE_INPUT_FILE = "hive-input.txt";
	private static final String JSON_TABLE_FILE = "table.json";
	private static final String FIELD_KEY = "key";
	private static final String FIELD_ID = "id";
	private static final String FIELD_VALUE = "value";

	public static void main(String[] args) throws IOException {
		SparkConf conf = SparkUtils.getLocalConfig(HiveSparkSQL.class.getSimpleName());
		log.info(String.format("access to the web interface at localhost: %s", SparkUtils.SPARK_UI_PORT));
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		String hiveInputFilepath = SparkUtils.getClasspathFileURI(HIVE_INPUT_FILE);
		String hiveQuery;

		HiveContext hiveContext = new HiveContext(sparkContext.sc());

		//TODO 
		//with none of these I'm able to set up the configuration programmatically, only placing hive-site.xml in the classpath works
		//		Properties properties = SparkUtils.loadProperties("/hive.properties");
		//		for (Object property : properties.keySet()) {
		//			String key = String.valueOf(property);
		//			String value = String.valueOf(properties.get(property));
		//			log.info(String.format("Setting (%s=%s)", key, value));
		//			hiveContext.hiveconf().set(key, value);
		//			hiveContext.conf().setConf(key, value);
		//			hiveContext.setConf(key, value);
		//		}

		//hive table creation
		hiveQuery = String.format("DROP TABLE IF EXISTS %s", HIVE_TABLE_NAME);
		log.info(String.format("Running query: %s", hiveQuery));
		hiveContext.sql(hiveQuery);

		hiveQuery = String.format("CREATE TABLE IF NOT EXISTS %s (%s INT, %s STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", HIVE_TABLE_NAME, FIELD_KEY,
				FIELD_VALUE);
		log.info(String.format("Running query: %s", hiveQuery));
		hiveContext.sql(hiveQuery);

		hiveQuery = String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE %s", hiveInputFilepath, HIVE_TABLE_NAME);
		log.info(String.format("Running query: %s", hiveQuery));
		hiveContext.sql(hiveQuery);

		hiveQuery = String.format("FROM %s SELECT key, value", HIVE_TABLE_NAME);
		log.info(String.format("Running query: %s", hiveQuery));
		DataFrame hiveQueryDataFrame = hiveContext.sql(hiveQuery);
		hiveQueryDataFrame.show();

		// Creating table from RDD / loading information into it
		//TODO find something smarter than dumping the content to a file and loading it hive style

		//create DataFrame from a file
		SQLContext sqlContext = new SQLContext(sparkContext);
		String jsonInputFilename = SparkUtils.getClasspathFileURI(JSON_TABLE_FILE);
		log.info(String.format("Loading the json file '%s' into a table:", jsonInputFilename));
		LoggerUtils.logFileContent(log, new File(SparkUtils.getClasspathFilePath(JSON_TABLE_FILE)));
		DataFrame jsonDataFrame = sqlContext.read().json(jsonInputFilename);
		jsonDataFrame.registerTempTable(JSON_TABLE_NAME);

		log.info(String.format("DataFrame for Table '%s' created from file %s:", JSON_TABLE_NAME, jsonInputFilename));
		jsonDataFrame.show();
		log.info(String.format("Table '%s' to join to", HIVE_TABLE_NAME));
		hiveQueryDataFrame.show();

		//join between dataframe and hive table
		Column joinExpr = new Column(hiveQueryDataFrame.col(FIELD_KEY).expr()).equalTo(jsonDataFrame.col(FIELD_ID));
		DataFrame joinResultDataFrame = hiveQueryDataFrame.join(jsonDataFrame, joinExpr);
		log.info(String.format("Joined table: %s with %s having %s=%s:", HIVE_TABLE_NAME, JSON_TABLE_NAME, FIELD_KEY, FIELD_ID));
		joinResultDataFrame.show();

		sparkContext.close();
	}
}
