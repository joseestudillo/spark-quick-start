package com.joseestudillo.spark;

import java.io.PrintWriter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.joseestudillo.spark.utils.SparkUtils;

/**
 * Simple example of an Spark Driver that runs locally
 * 
 * @author Jose Estudillo
 *
 */
public class SparkLocalDriver {

	private static final Logger log = LogManager.getLogger(SparkLocalDriver.class);
	private static final String SPARK_UI_PORT = "4040";

	public static void main(String[] args) throws InterruptedException {
		log.info(String.format("Starting %s", SparkTextSearch.class.getSimpleName()));
		SparkConf conf = SparkUtils.getLocalConfig(SparkTextSearch.class.getSimpleName());
		log.info(String.format("access to the web interface at localhost:%", SPARK_UI_PORT));
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		SparkUtils.listProperties(conf, new PrintWriter(System.out));
		Thread.sleep(60000);// time to be able to check the UI
		log.info(String.format("Done %s", SparkTextSearch.class.getSimpleName()));
	}
}
