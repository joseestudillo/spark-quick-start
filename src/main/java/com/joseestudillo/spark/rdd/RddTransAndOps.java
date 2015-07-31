package com.joseestudillo.spark.rdd;

import java.util.List;
import java.util.function.Function;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.actors.threadpool.Arrays;

import com.joseestudillo.spark.SparkTextSearch;
import com.joseestudillo.spark.utils.SparkUtils;

public class RddTransAndOps {
	
	private static final Logger log = Logger.getLogger(RddTransAndOps.class);
	
	public static void main(String[] args) {
		SparkConf conf = SparkUtils.getLocalConfig(SparkTextSearch.class.getSimpleName());
		log.info("access to the web interface at localhost:4040");
		JavaSparkContext spark = new JavaSparkContext(conf);
		String filepath = SparkUtils.getClasspathFileFullPath("numbers.txt");
		
		
		JavaRDD<String> textFileRdd = spark.textFile(filepath);
		log.info(textFileRdd.collect());
		JavaRDD<Integer> intFileRdd = textFileRdd.map((s) -> Integer.valueOf(s));
		log.info(intFileRdd.collect());
		Integer reduction = intFileRdd.reduce((a,b) -> a + b);
		JavaRDD<String> union = textFileRdd.union(intFileRdd.map(i -> Integer.toString(i)));
		
		// #map
		
		// #flatMap
		
		
		log.info(union.collect());
		// #sortBy
		//for sort by what you need is a function that give you a value to sort by, in this case we will use the string itself
		Function<String,String> functor = x -> x;  
		log.info(union.distinct().sortBy(functor,true,1).collect());
		Function<String,Integer> functor2 = x -> Integer.valueOf(x);
		log.info(union.distinct().sortBy(functor2,true,1).collect());
		
		// #union
		// #intersection
		// #substract
		// #cartesian
	}
}
