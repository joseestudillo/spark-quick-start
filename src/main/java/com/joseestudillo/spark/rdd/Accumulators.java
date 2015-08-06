package com.joseestudillo.spark.rdd;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.Accumulable;
import org.apache.spark.AccumulableParam;
import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.joseestudillo.spark.utils.SparkUtils;

public class Accumulators {

	private static final Logger log = LogManager.getLogger(Accumulators.class);

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

	//to accumulate data where the resulting type is not the same as the elements added AccumulableParam<Output, Input>
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
		SparkConf conf = SparkUtils.getLocalConfig(DoubleRDDs.class.getSimpleName());
		log.info("access to the web interface at localhost:4040");
		JavaSparkContext spark = new JavaSparkContext(conf);

		//Spark automatically deals with failed or slow machines by re-executing failed or slow tasks.
		//The net result is therefore that the same function may run multiple times on the same data depending on what happens on the cluster.

		//the ideal place to put accumulators is in actions as the accumulator will only be updated for succesful tasks
		//in transformations this is not guarantee as they can be run many times (caching, being part of the DAG in another RDD, ...)

		List<BigDecimal> bigDecimalList = Arrays.asList(new BigDecimal(0), new BigDecimal(1), new BigDecimal(2));
		List<Integer> intList = bigDecimalList.stream().map(x -> x.intValue()).collect(Collectors.toList());

		// #Predefined acumulator
		Accumulator<Integer> integerAccumulator = spark.intAccumulator(0);
		integerAccumulator.value();
		spark.parallelize(intList).foreach(x -> integerAccumulator.add(x));
		log.info(String.format("%s applied to %s -> %s", Accumulator.class.getName(), intList, integerAccumulator.value()));

		// #Custom accumulator
		Accumulator<BigDecimal> bigDecimalAccumulator = spark.accumulator(new BigDecimal(0), new BigDecimalAccumulator());
		spark.parallelize(bigDecimalList).foreach(x -> bigDecimalAccumulator.add(x));
		log.info(String.format("%s applied to %s -> %s", BigDecimalAccumulator.class.getName(), bigDecimalList, bigDecimalAccumulator.value()));

		// #Custom accumulable
		Accumulable<List<String>, BigDecimal> bigDecimalAccumulable = spark.accumulable(new ArrayList<String>(), new BigDecimalAccumulable());
		spark.parallelize(bigDecimalList).foreach(x -> bigDecimalAccumulable.add(x));
		log.info(String.format("%s applied to %s -> %s", BigDecimalAccumulable.class.getName(), bigDecimalList, bigDecimalAccumulable.value()));

		//TODO improve when it can be run in a cluster

		spark.close();

	}
}
