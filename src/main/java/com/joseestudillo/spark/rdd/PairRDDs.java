package com.joseestudillo.spark.rdd;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.joseestudillo.spark.utils.SparkUtils;

import scala.Tuple2;

public class PairRDDs {

	private static final Logger log = LogManager.getLogger(PairRDDs.class);

	public static void main(String[] args) {
		SparkConf conf = SparkUtils.getLocalConfig(PairRDDs.class.getSimpleName());
		log.info("access to the web interface at localhost:4040");
		JavaSparkContext spark = new JavaSparkContext(conf);

		List<Integer> integers = Stream.iterate(0, n -> n + 1).limit(10).collect(Collectors.toList());
		JavaRDD<Integer> intsRdd = spark.parallelize(integers);

		// #JavaPairRdd
		//PairFunction<Input Type, Tuple_1 type, Tuble_2 Type>
		PairFunction<Integer, String, Integer> pairFunction = a -> new Tuple2<String, Integer>(String.format("[%s]", a), a);
		JavaPairRDD<String, Integer> pairsRdd = intsRdd.mapToPair(pairFunction);
		log.info(String.format("Creating pairs from %s applying a -> ('[a]', a): %s", intsRdd.collect(), pairsRdd.collect()));

		List<Tuple2<String, Integer>> pairs0 = Arrays.asList(
				new Tuple2<String, Integer>("a", 1),
				new Tuple2<String, Integer>("b", 1),
				new Tuple2<String, Integer>("b", 2),
				new Tuple2<String, Integer>("c", 1),
				new Tuple2<String, Integer>("c", 2),
				new Tuple2<String, Integer>("c", 3));
		List<Tuple2<String, Integer>> pairs1 = Arrays.asList(
				new Tuple2<String, Integer>("d", 1),
				new Tuple2<String, Integer>("e", 1),
				new Tuple2<String, Integer>("e", 2),
				new Tuple2<String, Integer>("c", 1),
				new Tuple2<String, Integer>("c", 2),
				new Tuple2<String, Integer>("c", 3));

		JavaPairRDD<String, Integer> pairsRdd0 = JavaPairRDD.fromJavaRDD(spark.parallelize(pairs0));
		JavaPairRDD<String, Integer> pairsRdd1 = JavaPairRDD.fromJavaRDD(spark.parallelize(pairs1));

		// #Transformations

		// ##reduceByKey(func)
		log.info(String.format("%s reduceByKey +: %s", pairsRdd0.collect(), pairsRdd0.reduceByKey((a, b) -> a + b).collect()));

		// ##groupByKey()
		log.info(String.format("%s groupByKey(): %s", pairsRdd0.collect(), pairsRdd0.groupByKey().collect()));
		log.info(String.format("%s groupByKey(2 partitions): %s", pairsRdd0.collect(), pairsRdd0.groupByKey(2).collect()));

		// ##combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner)`
		Function<Integer, Double> createCombiner = a -> {
			Double result = new Double(a);
			log.info(String.format("%s -combiner-> %s", a, result));
			return result;
		};
		Function2<Double, Integer, Double> mergeValue = (i, d) -> {
			Double result = new Double(i) + d;
			log.info(String.format("(%s, %s) -mergeValues-> %s", i, d, result));
			return result;
		};
		Function2<Double, Double, Double> mergeCombiners = (d0, d1) -> {
			Double result = d0 + d1;
			log.info(String.format("(%s, %s) -mergeCombiners-> %s", d0, d1, result));
			return result;
		};
		Partitioner partitioner = new Partitioner() {

			@Override
			public int numPartitions() {
				return 2;
			}

			@Override
			public int getPartition(Object key) {
				int result = key.hashCode() % this.numPartitions();
				log.info(String.format("(%s) -partitioner-> %s", key, result));
				return result;
			}
		};
		log.info(String.format("%s combineByKey %s", pairsRdd0.collect(), pairsRdd0.combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner)
				.collect()));

		// ##mapValues(func)
		log.info(String.format("%s mapValues(+1) %s", pairsRdd0.collect(), pairsRdd0.mapValues(a -> a + 1).collect()));

		// ##flatMapValues: [(k, [v0, v1])] -> [(k,v0), (k, v1)]
		JavaPairRDD<String, List<Integer>> tmpPairsRdd = pairsRdd0.mapValues(a -> Arrays.asList(a, a));
		Function<List<Integer>, Iterable<Integer>> flattener = l -> l;
		log.info(String.format("%s flatMapValues(l -> l) %s", pairsRdd0.collect(), tmpPairsRdd.flatMapValues(flattener).collect()));

		// ##keys()
		log.info(String.format("%s keys %s", pairsRdd0.collect(), pairsRdd0.keys().collect()));

		// ##values()
		log.info(String.format("%s values %s", pairsRdd0.collect(), pairsRdd0.values().collect()));

		// ##sortByKey()`
		log.info(String.format("%s sortByKey %s", pairsRdd0.collect(), pairsRdd0.sortByKey().collect()));

		// #Operations

		// ##subtractByKey
		log.info(String.format("%s subtractByKey %s ==> %s", pairsRdd0.collect(), pairsRdd1.collect(), pairsRdd0.subtractByKey(pairsRdd1).collect()));

		// ##join
		//[(k,v0)] join [(k,v1), (k,v2)] ==>[(k, (v0, v1)), (k, (v0, v2))]
		log.info(String.format("%s join %s ==> %s", pairsRdd0.collect(), pairsRdd1.collect(), pairsRdd0.join(pairsRdd1).collect()));

		// ##rightOuterJoin
		log.info(String.format("%s rightOuterJoin %s ==> %s", pairsRdd0.collect(), pairsRdd1.collect(), pairsRdd0.rightOuterJoin(pairsRdd1).collect()));

		// ##leftOuterJoin
		log.info(String.format("%s leftOuterJoin %s ==> %s", pairsRdd0.collect(), pairsRdd1.collect(), pairsRdd0.leftOuterJoin(pairsRdd1).collect()));

		// ##cogroup   ==> foreach key (k, (list of the asssociated values in the right side, list of the associated values on the left side))
		log.info(String.format("%s cogroup %s ==> %s", pairsRdd0.collect(), pairsRdd1.collect(), pairsRdd0.cogroup(pairsRdd1).collect()));

		// #Aggregations

		// ##foldByKey
		Function2<Integer, Integer, Integer> keyFolder = (a, b) -> a + b;
		log.info(String.format("%s foldByKey(+) %s ", pairsRdd0.collect(), pairsRdd0.foldByKey(0, keyFolder).collect()));

		// ##reduceByKey
		log.info(String.format("%s reduceByKey(+) %s ", pairsRdd0.collect(), pairsRdd0.reduceByKey((a, b) -> a + b).collect()));

		// ##combineByKey
		log.info(String.format("%s combineByKey(+) %s ", pairsRdd0.collect(), pairsRdd0.combineByKey(createCombiner, mergeValue, mergeCombiners).collect()));

		// #
		//countByKey()
		log.info(String.format("%s countByKey %s ", pairsRdd0.collect(), pairsRdd0.countByKey()));
		//collectAsMap()
		//log.info(String.format("%s collectAsMap %s ", pairsRdd0.collect(), pairsRdd0.collectAsMap()));
		//lookup(key)
		log.info(String.format("%s lookup(c) %s ", pairsRdd0.collect(), pairsRdd0.lookup("c")));

		spark.close();
	}
}
