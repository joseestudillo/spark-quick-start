package com.joseestudillo.spark.rdd;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.joseestudillo.spark.utils.SparkUtils;

/**
 * 
 * Broadcast variable examples
 * 
 * Broadcast vars allow you to have a read-only value in all the workers avoiding the need of sending them every time they are needed. the value of the property
 * {@code spark.serializer} affects to this, as the values of broadcast variables must be serialized to be sent to all the workers
 * 
 * @author Jose Estudillo
 *
 */
public class BroadcastVariables {

	private final static Logger log = Logger.getLogger(BroadcastVariables.class);

	public static void main(String[] args) {
		SparkConf conf = SparkUtils.getLocalConfig(BroadcastVariables.class.getSimpleName());
		log.info(String.format("access to the web interface at localhost:%s", SparkUtils.SPARK_UI_PORT));
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		List<Integer> integers = Stream.iterate(0, n -> n + 1).limit(100).collect(Collectors.toList());

		//creation
		Broadcast<List<Integer>> broadcastedIntList = sparkContext.broadcast(integers);

		//access
		broadcastedIntList.getValue();

		sparkContext.close();
	}

}
