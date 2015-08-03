package com.joseestudillo.spark.rdd;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;

import com.joseestudillo.spark.utils.SparkUtils;

import scala.Tuple2;

public class Partitioning {

	private static final Logger log = Logger.getLogger(Partitioning.class);

	private static class CustomPartitioner extends Partitioner {

		@Override
		public int getPartition(Object key) {
			int result = key.hashCode() % this.numPartitions();
			log.info(String.format("Partition: %s -> %s", key, result));
			return result;
		}

		@Override
		public int numPartitions() {
			return 3;
		}

		/**
		 * This is important to implement because Spark will need to test your Partitioner object against other instances of itself when it decides whether two
		 * of your RDDs are partitioned the same way!
		 */
		@Override
		public boolean equals(Object obj) {
			return obj instanceof CustomPartitioner && this.numPartitions() == ((CustomPartitioner) obj).numPartitions();
		}

	}

	public static void main(String[] args) {
		SparkConf conf = SparkUtils.getLocalConfig(DoubleRDDs.class.getSimpleName());
		log.info("access to the web interface at localhost:4040");
		JavaSparkContext spark = new JavaSparkContext(conf);

		List<Tuple2<String, Integer>> pairs0 = Arrays.asList(
				new Tuple2<String, Integer>("a", 1),
				new Tuple2<String, Integer>("b", 1),
				new Tuple2<String, Integer>("b", 2),
				new Tuple2<String, Integer>("c", 1),
				new Tuple2<String, Integer>("c", 2),
				new Tuple2<String, Integer>("c", 3));

		JavaPairRDD<String, Integer> pairsRdd = JavaPairRDD.fromJavaRDD(spark.parallelize(pairs0));

		PairFlatMapFunction<Iterator<Tuple2<String, Integer>>, Set<String>, Integer> pairFlapMapper = new PairFlatMapFunction<Iterator<Tuple2<String, Integer>>, Set<String>, Integer>() {

			@Override
			public Iterable<Tuple2<Set<String>, Integer>> call(Iterator<Tuple2<String, Integer>> tuples) throws Exception {
				int size = 0;
				Set<String> keySet = new HashSet<String>();
				for (; tuples.hasNext();) {
					Tuple2<String, Integer> tuple = tuples.next();
					keySet.add(tuple._1);
					size++;

				}
				return Arrays.asList(new Tuple2(keySet, size));
			}

		};
		log.info(String.format("Original RDD: %s", pairsRdd.collect()));
		log.info(String.format("Original RDD parition Info (keys, #elements): %s", pairsRdd.mapPartitionsToPair(pairFlapMapper).collect()));
		JavaPairRDD<String, Integer> partitionedPairsRdd = pairsRdd.partitionBy(new CustomPartitioner()).persist(StorageLevel.MEMORY_ONLY());
		//when partitioning, It is important to check the persistence op have been performed, otherwise the partition operations is repeated every time.

		log.info(String.format("Partitioned RDD partition Info (keys, #elements): %s", partitionedPairsRdd.mapPartitionsToPair(pairFlapMapper).collect()));

		Function2<Integer, Iterator<Tuple2<String, Integer>>, Iterator<Tuple2<Integer, Integer>>> flatter = (index, tuplesIter) -> {
			int size = 0;
			for (; tuplesIter.hasNext();) {
				Tuple2<String, Integer> tuple = tuplesIter.next();
				size++;
			}
			return Arrays.asList(new Tuple2<Integer, Integer>(index, size)).iterator();
		};

		log.info(String.format("Original RDD parition Info (PartitionIndex, #Elements): %s", pairsRdd.mapPartitionsWithIndex(flatter, true).collect()));
		log.info(String.format("Partitioned RDD partition Info (PartitionIndex, #Elements): %s", partitionedPairsRdd.mapPartitionsWithIndex(flatter, true)
				.collect()));
	}
}
