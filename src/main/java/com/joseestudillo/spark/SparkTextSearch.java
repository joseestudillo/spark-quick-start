package com.joseestudillo.spark;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.joseestudillo.spark.utils.SparkUtils;

/**
 * Modification from the original example given in the official Spark documentation
 * 
 * @author Jose Estudillo
 *
 */
public class SparkTextSearch {

	private static final Logger log = LogManager.getLogger(SparkTextSearch.class);

	public static Function<String, Boolean> containsFilter(final String searchStr) {
		return s -> s.contains(searchStr);
	}

	public static void main(String[] args) {

		log.info("Spark Text Search");

		String filepath = SparkTextSearch.class.getClassLoader().getResource("") + "/file.txt";
		final String search0 = "ERROR";
		final String search1 = "1";

		log.info(String.format("searching in the content of the file %s", filepath));

		SparkConf conf = SparkUtils.getLocalConfig(SparkTextSearch.class.getSimpleName());
		JavaSparkContext spark = new JavaSparkContext(conf);
		JavaRDD<String> textFile = spark.textFile(filepath);

		Function<String, Boolean> filter0 = containsFilter(search0);
		Function<String, Boolean> filter1 = containsFilter(search1);

		log.info(String.format("Filtering by %s", search0));
		JavaRDD<String> results = textFile.filter(filter0);

		// Count all the results
		long count = results.count();
		log.info(String.format("%s results found", count));

		// Count errors mentioning MySQL
		log.info(String.format("Filtering the results by %s", search1));
		count = results.filter(filter1).count();
		log.info(String.format("%s results found", count));

		// Fetch the MySQL errors as an array of strings
		List<String> resultsCollection = results.filter(filter1).collect();
		log.info(String.format("results: %s", resultsCollection));

		log.info("Stopping spark...");
		spark.stop();
		spark.close();
		log.info("Done");
	}
}
