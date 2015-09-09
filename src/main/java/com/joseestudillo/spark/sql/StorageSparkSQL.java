package com.joseestudillo.spark.sql;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.joseestudillo.spark.SparkTextSearch;
import com.joseestudillo.spark.utils.SparkUtils;

/**
 * Example of loading/saving DataFrames to/from Spark
 * 
 * @author Jose Estudillo
 *
 */
public class StorageSparkSQL {

	private static final Logger log = Logger.getLogger(StorageSparkSQL.class);

	private static final String TABLE_NAME = "json_table";
	private static final String FIELD_VALUE = "value";
	private static final String FIELD_KEY = "key";

	private static final String SPARK_PARQUET_FORMAT = "parquet";

	private static final String JSON_TABLE_FILENAME = "table.json";
	private static final String PARQUET_QUERY_FILENAME = "/tmp/query.parquet";
	private static final String PARQUET_SCHEMA_FILENAME = "/tmp/schema.parquet";

	public static void main(String[] args) throws IOException {
		SparkConf conf = SparkUtils.getLocalConfig(SparkTextSearch.class.getSimpleName());
		log.info(String.format("access to the web interface at localhost: %s", SparkUtils.SPARK_UI_PORT));
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(sparkContext);

		String jsonFilePath = SparkUtils.getClasspathFileURI(JSON_TABLE_FILENAME);
		log.info(String.format("Loading data from %s", jsonFilePath));
		DataFrame jsonDataFrame = sqlContext.read().json(jsonFilePath);
		jsonDataFrame.registerTempTable(TABLE_NAME); //this gives a name to the table making it accessible

		File queryParquetFile = new File(PARQUET_QUERY_FILENAME);
		File schemaParquetFile = new File(PARQUET_SCHEMA_FILENAME);
		for (File file : Arrays.asList(queryParquetFile, schemaParquetFile)) {
			if (file.exists()) {
				FileUtils.deleteDirectory(file);
			}
		}

		//Saving the result of a query
		log.info(String.format("Storing DataFrame query into %s using parquet", queryParquetFile));
		jsonDataFrame.select(FIELD_VALUE).write().format(SPARK_PARQUET_FORMAT).save(queryParquetFile.getPath());

		//Loading the result of a query from a parquet file
		log.info(String.format("Loading data from parquet file: %s", queryParquetFile));
		DataFrame loadedParquetFileQueryDataFrame = sqlContext.read().parquet(queryParquetFile.getPath());
		log.info("Loaded DataFrame:");
		loadedParquetFileQueryDataFrame.show();

		//Saving a whole dataframe
		log.info(String.format("Storing a complete DataFrame into %s using parquet", schemaParquetFile));
		jsonDataFrame.write().parquet(schemaParquetFile.getPath());

		//Loading a whole dataframe from a parquet file
		log.info(String.format("Loading %s into a DataFrame", schemaParquetFile));
		DataFrame loadedParquetFileDataFrame = sqlContext.read().parquet(schemaParquetFile.getPath());
		log.info("Loaded DataFrame:");
		loadedParquetFileDataFrame.show();
	}
}
