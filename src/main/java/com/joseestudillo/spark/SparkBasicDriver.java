package com.joseestudillo.spark;

import java.io.PrintWriter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.joseestudillo.spark.utils.SparkUtils;

/**
 * Simple example of an Spark Driver to run in a remote cluster. The configuration is not set up at all.
 * 
 * @author Jose Estudillo
 *
 */
public class SparkBasicDriver {

	private static final Logger log = Logger.getLogger(SparkBasicDriver.class);

	public static void main(String[] args) throws InterruptedException {
		String appName = SparkBasicDriver.class.getSimpleName() + "_" + System.currentTimeMillis();
		log.info(String.format("Starting %s", SparkBasicDriver.class));
		SparkConf conf = new SparkConf().setAppName(appName);
		log.info(String.format("access to the web interface at localhost:%s", SparkUtils.SPARK_UI_PORT));
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		SparkUtils.listConfigurationProperties(conf, new PrintWriter(System.out));
		//Thread.sleep(60000);// time to be able to check the UI

		List<Integer> integers = Stream.iterate(0, n -> n + 1).limit(10).collect(Collectors.toList());
		JavaRDD<Integer> intsRdd = sparkContext.parallelize(integers);
		log.info(String.format("Input integers collection: %s", intsRdd.collect()));

		// # map
		JavaRDD<String> stringsRdd = intsRdd.map((i) -> Integer.toString(i));
		log.info(String.format("map: Integer -> String: %s", stringsRdd.collect()));

		// # reduction
		Integer intReduction = intsRdd.reduce((a, b) -> a + b);
		String stringReduction = stringsRdd.reduce((a, b) -> a + b);
		log.info(String.format("Integers %s reduced using + to %s", intsRdd.collect(), intReduction));
		log.info(String.format("Strings %s reduced using + to %s", stringsRdd.collect(), stringReduction));

		sparkContext.close();
		log.info(String.format("Done %s", appName));
	}
}
