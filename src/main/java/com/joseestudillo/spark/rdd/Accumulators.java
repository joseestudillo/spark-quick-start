package com.joseestudillo.spark.rdd;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.Accumulable;
import org.apache.spark.AccumulableParam;
import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.joseestudillo.spark.utils.SparkUtils;

/**
 * Accumulator examples
 * <p>
 * Spark automatically deals with failed or slow machines by re-executing failed or slow tasks. This means that the same transformation may run multiple times
 * (caching, being part of the DAG in another RDD, ...) on the same data depending on what happens on the cluster. Because of this, the ideal place to put
 * accumulators is in actions as the accumulator will only be updated for successful tasks.
 *
 * @author Jose Estudillo
 */
public class Accumulators {

	private static final Logger log = Logger.getLogger(Accumulators.class);

	//to accumulate an specific type
	@SuppressWarnings("serial")
	private static class BigDecimalAccumulator implements AccumulatorParam<BigDecimal> {
		//TODO concurrency issues?
		@Override
		public BigDecimal zero(BigDecimal initialValue) {
			//do initialization
			return initialValue;
		}

		@Override
		public BigDecimal addInPlace(BigDecimal bd0, BigDecimal bd1) {
			return bd0.add(bd1);
		}

		@Override
		public BigDecimal addAccumulator(BigDecimal bd0, BigDecimal bd1) {
			return bd0.add(bd1);
		}

	}

	//to accumulate data where the resulting type is not the same as the elements added: AccumulableParam<Output, Input>
	@SuppressWarnings("serial")
	private static class BigDecimalAccumulable implements AccumulableParam<List<String>, BigDecimal> {
		//TODO concurrency issues?
		@Override
		public List<String> addAccumulator(List<String> l0, BigDecimal e) {
			l0.add(String.valueOf(e));
			return l0;
		}

		@Override
		public List<String> addInPlace(List<String> l0, List<String> l1) {
			l0.addAll(l1);
			return l0;
		}

		@Override
		public List<String> zero(List<String> initialValue) {
			//do initialization
			return initialValue;
		}
	}

	public static void main(String[] args) {
		SparkConf conf = SparkUtils.getLocalConfig(Accumulators.class.getSimpleName());
		log.info(String.format("access to the web interface at localhost: %s", SparkUtils.SPARK_UI_PORT));
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		List<BigDecimal> bigDecimalList = Arrays.asList(new BigDecimal(0), new BigDecimal(1), new BigDecimal(2));
		List<Integer> intList = bigDecimalList.stream().map(BigDecimal::intValue).collect(Collectors.toList());

		// #Predefined accumulator that adds Integers
		Accumulator<Integer> integerAccumulator = sparkContext.intAccumulator(0);
		integerAccumulator.value();
		sparkContext.parallelize(intList).foreach(integerAccumulator::add);
		log.info(String.format("%s applied (Integer +) to: %s -> %s", Accumulator.class.getName(), intList, integerAccumulator.value()));

		// #Custom accumulator that adds BigDecimals 
		Accumulator<BigDecimal> bigDecimalAccumulator = sparkContext.accumulator(new BigDecimal(0), new BigDecimalAccumulator());
		sparkContext.parallelize(bigDecimalList).foreach(bigDecimalAccumulator::add);
		log.info(String.format("%s applied (BigDecimal +) to: %s -> %s", BigDecimalAccumulator.class.getName(), bigDecimalList, bigDecimalAccumulator.value()));

		// #Custom accumulable that adds the elements in a list
		Accumulable<List<String>, BigDecimal> bigDecimalAccumulable = sparkContext.accumulable(new ArrayList<String>(), new BigDecimalAccumulable());
		sparkContext.parallelize(bigDecimalList).foreach(bigDecimalAccumulable::add);
		log.info(String.format("%s applied (List<String> add) to: %s -> %s", BigDecimalAccumulable.class.getName(), bigDecimalList, bigDecimalAccumulable
				.value()));

		//TODO improve when it can be run in a cluster

		sparkContext.close();
	}
}
