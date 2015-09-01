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

	public static void main(String[] args) throws InterruptedException {
		String appName = SparkTextSearch.class.getSimpleName();
		log.info(String.format("Starting %s", appName));
		SparkConf conf = SparkUtils.getLocalConfig(appName);
		log.info(String.format("access to the web interface at localhost:%s", SparkUtils.SPARK_UI_PORT));
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		SparkUtils.listConfigurationProperties(conf, new PrintWriter(System.out));
		Thread.sleep(60000);// time to be able to check the UI
		log.info(String.format("Done %s", appName));
	}
}
