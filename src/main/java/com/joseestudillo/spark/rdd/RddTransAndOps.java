package com.joseestudillo.spark.rdd;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import com.joseestudillo.spark.SparkTextSearch;
import com.joseestudillo.spark.utils.SparkUtils;

/**
 * 
 * @author Jose Estudillo
 *
 */
public class RddTransAndOps {

	private static final Logger log = LogManager.getLogger(RddTransAndOps.class);

	public static void main(String[] args) {
		SparkConf conf = SparkUtils.getLocalConfig(SparkTextSearch.class.getSimpleName());
		log.info("access to the web interface at localhost:4040");
		JavaSparkContext spark = new JavaSparkContext(conf);

		// # Load info from text files
		String filepath = SparkUtils.getClasspathFileFullPath("numbers.txt");
		JavaRDD<String> stringsRdd;
		stringsRdd = spark.textFile(filepath);

		// # Loading information in a collection to spark
		List<Integer> integers = Stream.iterate(0, n -> n + 1).limit(10).collect(Collectors.toList());
		JavaRDD<Integer> intsRdd = spark.parallelize(integers);
		log.info(String.format("Input integers collection: %s", intsRdd));

		// #map
		stringsRdd = intsRdd.map((i) -> Integer.toString(i));
		log.info(String.format("maps the integers into strings", stringsRdd.collect()));

		// # reduction
		Integer intReduction = intsRdd.reduce((a, b) -> a + b);
		String stringReduction = stringsRdd.reduce((a, b) -> a + b);
		log.info(String.format("Integers %s reduced using + to %s", intsRdd.collect(), intReduction));
		log.info(String.format("Strings %s reduced using + to %s", stringsRdd.collect(), stringReduction));

		// #aggregate

		/* given type is Integer and result type will be string, you need two functions, one to mix integers with strings, and another one to mix strings */
		Function2<String, Integer, String> givenTypeCombiner = (a, b) -> String.format("(%s,%s)", a, Integer.toString(b)); //combine a given type with the result type
		Function2<String, String, String> resultTypeCombiner = (a, b) -> String.format("[%s,%s]", a, b);// combine two result types
		String aggregation = intsRdd.aggregate("", givenTypeCombiner, resultTypeCombiner);
		log.info(String.format("Aggregation: %s", aggregation));

		// #count
		log.info(String.format("Count of %s: %s", intsRdd.collect(), intsRdd.count()));

		// #countByValue
		log.info(String.format("Count by value of %s: %s", intsRdd.collect(), intsRdd.countByValue()));

		// #union
		JavaRDD<Integer> intsRdd2 = spark.parallelize(Stream.iterate(0, n -> n + 1).limit(5).collect(Collectors.toList()));
		JavaRDD<Integer> unionRdd = intsRdd.union(intsRdd2);
		log.info(String.format("Union(%s, %s): %s", intsRdd.collect(), intsRdd2.collect(), unionRdd.collect()));

		// #intersection
		log.info(String.format("Intersection(%s, %s): %s", intsRdd.collect(), intsRdd2.collect(), intsRdd.intersection(intsRdd2).collect()));

		// #substract
		log.info(String.format("Substract(%s, %s): %s", intsRdd.collect(), intsRdd2.collect(), intsRdd.subtract(intsRdd2).collect()));

		// #cartesian
		log.info(String.format("Cartersian(%s, %s): %s", intsRdd.collect(), intsRdd2.collect(), intsRdd.cartesian(intsRdd2).collect()));

		// #sortBy
		log.info(String.format("Unsorted union result: %s", unionRdd.collect()));
		//for sortBy what you need is a function that give you a value to sort by (functor), in this case we will use the string itself

		Function<Integer, ?> functor = x -> x;
		JavaRDD<Integer> sortedRdd = unionRdd.sortBy(functor, true, 1);
		log.info(String.format("Collection sorted %s", sortedRdd.collect()));

		// #distinct
		log.info(String.format("Removing repeated elements: %s Notice that 'distinct' mess up the sorted rdd", sortedRdd.distinct().collect()));

		// #flatMap
		JavaRDD<List<Integer>> listOfLists = intsRdd.map(x -> Arrays.asList(x, x));

		org.apache.spark.api.java.function.Function<List<Integer>, ?> mapper = x -> Arrays.asList(x.stream().reduce(Integer::sum).get());
		FlatMapFunction<List<Integer>, ?> flatMapper = x -> Arrays.asList(x.stream().reduce(Integer::sum).get());
		//notice that mapper and flatMapper are the same function, they just cant be casted to each other.
		log.info(String.format("to map: %s", listOfLists.collect()));
		log.info(String.format("map sum(List<Int>): %s", listOfLists.map(mapper).collect()));
		log.info(String.format("flatMap sum(List<Int>): %s", listOfLists.flatMap(flatMapper).collect()));
	}
}
