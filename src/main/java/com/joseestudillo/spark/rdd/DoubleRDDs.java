package com.joseestudillo.spark.rdd;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.joseestudillo.spark.utils.SparkUtils;

/**
 * DoubleRDD examples
 * 
 * DoubleRDDs have some special operations for numbers like mean or variance
 * 
 * @author Jose Estudillo
 *
 */
public class DoubleRDDs {

	private static final Logger log = Logger.getLogger(DoubleRDDs.class);

	public static void main(String[] args) {
		SparkConf conf = SparkUtils.getLocalConfig(DoubleRDDs.class.getSimpleName());
		log.info(String.format("access to the web interface at localhost: %s", SparkUtils.SPARK_UI_PORT));
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		// #DoubleRDD
		List<Integer> integers = Stream.iterate(0, n -> n + 1).limit(10).collect(Collectors.toList());
		JavaRDD<Integer> intsRdd = sparkContext.parallelize(integers);
		JavaDoubleRDD doubleRdd = intsRdd.mapToDouble(a -> new Double(a));
		log.info(String.format("DoubleRdd:%s mean:%s variance:%s", doubleRdd.collect(), doubleRdd.mean(), doubleRdd.variance()));
		log.info(String.format("DoubleRdd:%s statistics:%s", doubleRdd.collect(), doubleRdd.stats()));

		sparkContext.close();
	}
}
