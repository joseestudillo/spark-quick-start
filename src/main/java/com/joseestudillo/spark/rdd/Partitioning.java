package com.joseestudillo.spark.rdd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;

import com.joseestudillo.spark.utils.SparkUtils;

import scala.Tuple2;

/**
 * Partitioning Examples
 * 
 * @author Jose Estudillo
 *
 */
public class Partitioning {

	private static final Logger log = Logger.getLogger(Partitioning.class);

	@SuppressWarnings("serial")
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

		//TODO redefine hashCode
	}

	public static void main(String[] args) {
		SparkConf conf = SparkUtils.getLocalConfig(Partitioning.class.getSimpleName());
		log.info(String.format("access to the web interface at localhost: %s", SparkUtils.SPARK_UI_PORT));
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		List<Tuple2<String, Integer>> pairs0 = Arrays.asList(
				new Tuple2<String, Integer>("a", 1),
				new Tuple2<String, Integer>("b", 1),
				new Tuple2<String, Integer>("b", 2),
				new Tuple2<String, Integer>("c", 1),
				new Tuple2<String, Integer>("c", 2),
				new Tuple2<String, Integer>("c", 3));

		JavaPairRDD<String, Integer> pairsRdd = JavaPairRDD.fromJavaRDD(sparkContext.parallelize(pairs0));

		// #mapPartitionsToPair. Allows to apply a map function to all the elements in a partition. It must return a a list of pairs.

		@SuppressWarnings("serial")
		/**
		 * Adds 1 to all the values in the tuples and convert the values to strings.
		 * 
		 * Basically Iterator<Tuple2<String, Integer>> -> Tuple2<String, String>
		 * 
		 * This function is called once for each partition
		 */
		PairFlatMapFunction<Iterator<Tuple2<String, Integer>>, String, String> mapPartitionsToPairFuncPlusOne = new PairFlatMapFunction<Iterator<Tuple2<String, Integer>>, String, String>() {

			@Override
			public Iterable<Tuple2<String, String>> call(Iterator<Tuple2<String, Integer>> tuples) throws Exception {
				List<Tuple2<String, Integer>> inputTupleList = new ArrayList<>();
				List<Tuple2<String, String>> outputTupleList = new ArrayList<>();
				for (; tuples.hasNext();) {
					Tuple2<String, Integer> tuple = tuples.next();
					inputTupleList.add(tuple);
					outputTupleList.add(new Tuple2<String, String>(tuple._1, String.valueOf(tuple._2 + 1)));

				}
				log.info(String.format("mapPartitionsToPairFuncPlusOne.call gets -> %s returns -> %s", inputTupleList, outputTupleList));
				return (outputTupleList.size() > 0) ? outputTupleList : Collections.emptyList();
			}
		};

		log.info(String.format("Original RDD: %s", pairsRdd.collect()));
		SparkUtils.logPartitionInformation(log, pairsRdd);
		log.info(String.format("Original RDD partition Info (Tuple Value +1): %s", pairsRdd.mapPartitionsToPair(mapPartitionsToPairFuncPlusOne).collect()));

		JavaPairRDD<String, Integer> partitionedPairsRdd = pairsRdd.partitionBy(new CustomPartitioner()).persist(StorageLevel.MEMORY_ONLY());
		//when partitioning, It is important to check the persistence operation has been performed, otherwise the partition operations will be repeated every time.

		log.info(String.format("Custom Partitioned RDD: %s", partitionedPairsRdd.collect()));
		SparkUtils.logPartitionInformation(log, partitionedPairsRdd);
		log.info(String.format("Custom Partitioned RDD. mapPartitionsToPair( List<Tuples> -> List<Tuples with Value +1>): %s", partitionedPairsRdd
				.mapPartitionsToPair(
						mapPartitionsToPairFuncPlusOne)
				.collect()));

		// #mapPartitionsWithIndex. Allow to apply a map function to all the elements in each partition. It also provides the partition index.

		@SuppressWarnings("resource")
		/**
		 * Returns a list with pairs (partition index, #elements)
		 * 
		 * #mapPartitionsWithIndex() <partition index, Iterator of the elements in that partition>: f: (Int, Iterator[T]) -> Iterator[U]
		 */
		Function2<Integer, Iterator<Tuple2<String, Integer>>, Iterator<Tuple2<Integer, Integer>>> mapPartitionsWithIndexFunc = (index, tuplesIter) -> {
			int size = 0;
			List<Tuple2<String, Integer>> tupleList = new ArrayList<>();
			for (; tuplesIter.hasNext();) {
				Tuple2<String, Integer> tuple = tuplesIter.next();
				tupleList.add(tuple);
				size++;
			}
			log.info(String.format("mapPartitionsWithIndexFunc.call called with -> %s", tupleList));
			return Arrays.asList(new Tuple2<Integer, Integer>(index, size)).iterator();
		};

		log.info(String.format("Original RDD mapPartitionsWithIndex(partitionIndex, List<Tuples>) -> (PartitionIndex, #Elements): %s", pairsRdd
				.mapPartitionsWithIndex(mapPartitionsWithIndexFunc, true).collect()));

		log.info(String.format("Custom Partitioned RDD. mapPartitionsWithIndex(partitionIndex, List<Tuples>) -> (PartitionIndex, #Elements)): %s",
				partitionedPairsRdd.mapPartitionsWithIndex(mapPartitionsWithIndexFunc, true).collect()));

		// #mapPartitions. allow to apply a map function (Tuple -> value) to all the elements in each partition

		@SuppressWarnings("serial")
		/**
		 * Returns the list of all the tuple keys for each each partition. (Tuple -> key)
		 *
		 * mapPartitionsFunc <Iterator of the elements in a partition, Type of the elements to return in a list>. `f: (Iterator[T]) -> Iterator[U]`
		 * 
		 */
		FlatMapFunction<Iterator<Tuple2<String, Integer>>, String> mapPartitionsFunc = new FlatMapFunction<Iterator<Tuple2<String, Integer>>, String>() {
			@Override
			public Iterable<String> call(Iterator<Tuple2<String, Integer>> tuplesIter) throws Exception {
				List<String> keylist = new ArrayList<String>();
				for (; tuplesIter.hasNext();) {
					keylist.add(tuplesIter.next()._1);
				}
				log.info(String.format("mapPartitionsFunc.call returns -> %s", keylist));
				return keylist;
			}

		};
		log.info(String.format("Custom Partitioned RDD. mapPartitions(Tuple -> Key). (Keys): %s", partitionedPairsRdd.mapPartitions(mapPartitionsFunc)
				.collect()));

		// #foreachPartition. allows to go through all the elements in each partition

		/**
		 * Shows the elements contained in each partition
		 * 
		 * foreachPartitionFunc <Iterator of the elements in a partition>
		 */
		@SuppressWarnings("serial")
		VoidFunction<Iterator<Tuple2<String, Integer>>> foreachPartitionFunc = new VoidFunction<Iterator<Tuple2<String, Integer>>>() {

			@Override
			public void call(Iterator<Tuple2<String, Integer>> tuplesIter) throws Exception {
				List<Tuple2<String, Integer>> tupleList = new ArrayList<>();
				for (; tuplesIter.hasNext();) {
					tupleList.add(tuplesIter.next());
				}
				log.info(String.format("foreachPartitionFunc called with -> %s", tupleList));
			}
		};
		partitionedPairsRdd.foreachPartition(foreachPartitionFunc);

		sparkContext.close();
	}
}
