package com.joseestudillo.spark.rdd;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import com.joseestudillo.spark.SparkTextSearch;
import com.joseestudillo.spark.utils.SparkUtils;

/**
 * Example about how persistece is managed in Spark.
 * 
 * For further detail look at {@link http://spark.apache.org/docs/latest/programming-guide.html#which-storage-level-to-choose}
 * 
 * @author Jose Estudillo
 *
 */
public class Persistence {

	private static final Logger log = Logger.getLogger(Persistence.class);

	public static void main(String[] args) {
		SparkConf conf = SparkUtils.getLocalConfig(SparkTextSearch.class.getSimpleName());
		log.info("access to the web interface at localhost:4040");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		List<Integer> integers = Stream.iterate(0, n -> n + 1).limit(10).collect(Collectors.toList());
		JavaRDD<Integer> intsRdd = sparkContext.parallelize(integers);
		log.info(String.format("Input integers collection: %s", intsRdd));

		intsRdd.persist(StorageLevel.MEMORY_ONLY()); // this is similar to intsRdd.cache();

		//with <code>persist</code> you get to choose how and where (mem or disk) the data is stored.
	}
}
