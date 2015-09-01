package com.joseestudillo.spark.rdd;

import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.joseestudillo.spark.utils.SparkUtils;

/**
 * Piping example
 * 
 * @author Jose Estudillo
 *
 */
public class Piping {

	private static final Logger log = LogManager.getLogger(Piping.class);

	public static void main(String[] args) {
		SparkConf conf = SparkUtils.getLocalConfig(DoubleRDDs.class.getSimpleName());
		log.info(String.format("access to the web interface at localhost: %s", SparkUtils.SPARK_UI_PORT));
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		String command = "grep -E [0-9]+";
		JavaRDD<String> inputRdd = sparkContext.parallelize(Arrays.asList("1", "2", "3", "a", "b", "c"));
		JavaRDD<String> outputRdd = inputRdd.pipe(command);
		log.info(String.format("Applying '%s' to the input %s -> %s", command, inputRdd.collect(), outputRdd.collect()));

		sparkContext.close();
	}
}
