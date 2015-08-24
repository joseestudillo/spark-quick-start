package com.joseestudillo.spark.sql;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.joseestudillo.spark.SparkTextSearch;
import com.joseestudillo.spark.utils.SparkUtils;

/**
 * Example of loading/saving data to/from Spark
 * 
 * @author Jose Estudillo
 *
 */
public class LoadSaveSparkSQL {

	private static final Logger log = LogManager.getLogger(LoadSaveSparkSQL.class);

	public static void main(String[] args) throws IOException {
		SparkConf conf = SparkUtils.getLocalConfig(SparkTextSearch.class.getSimpleName());
		log.info("access to the web interface at localhost:4040");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(sparkContext);

		String tableName = "jsonTable";
		String field0 = "value";
		String field1 = "key";
		String jsonFilePath = SparkUtils.getClasspathFileFullPath("table.json");
		log.info(String.format("Loading data from %s", jsonFilePath));
		DataFrame jsonDataFrame = sqlContext.read().json(jsonFilePath);
		jsonDataFrame.registerTempTable(tableName); //this gives a name to the table making it accessible

		//Saving data
		File queryParquetFile = new File("/tmp/query.parquet");
		if (queryParquetFile.exists()) {
			FileUtils.deleteDirectory(queryParquetFile);
		}
		File schemaParquetFile = new File("/tmp/schema.parquet");
		if (schemaParquetFile.exists()) {
			FileUtils.deleteDirectory(schemaParquetFile);
		}
		log.info(String.format("Storing data from an RDD query into %s", queryParquetFile));
		jsonDataFrame.select(field0).write().format("parquet").save(queryParquetFile.getPath());
		DataFrame loadedParquetFileQueryDataFrame = sqlContext.read().parquet(queryParquetFile.getPath());
		log.info("Loaded DataFrame:");
		loadedParquetFileQueryDataFrame.show();

		log.info(String.format("Storing data from the DataFrame into %s", schemaParquetFile));
		jsonDataFrame.write().parquet(schemaParquetFile.getPath());
		log.info(String.format("Loading %s into a DataFrame", schemaParquetFile));
		DataFrame loadedParquetFileDataFrame = sqlContext.read().parquet(schemaParquetFile.getPath());
		log.info("Loaded DataFrame:");
		loadedParquetFileDataFrame.show();
	}
}
