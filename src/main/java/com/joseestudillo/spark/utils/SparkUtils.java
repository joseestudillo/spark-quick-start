package com.joseestudillo.spark.utils;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;

import com.joseestudillo.spark.SparkTextSearch;

import scala.Tuple2;

/**
 * 
 * Utility class for Spark
 * 
 * @author Jose Estudillo
 *
 */
public class SparkUtils {

	public static final String LOCAL_MASTER_ID = "local[*]";
	public static final String SPARK_UI_PORT = "4040";
	public static final String UNKNOWN_HOST = "Unknown";

	public static SparkConf getLocalConfig(String appName) {
		return new SparkConf().setAppName(appName).setMaster(LOCAL_MASTER_ID);
	}

	public static void listConfigurationProperties(SparkConf conf, PrintWriter pw) {
		for (Tuple2<String, String> tuple : conf.getAll()) {
			pw.format("%s: %s\n", tuple._1, tuple._2);
		}
		pw.flush();
	}

	public static String getUsername() {
		return System.getProperty("user.name");
	}

	public static String getHostname() {
		try {
			return java.net.InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			return UNKNOWN_HOST;
		}
	}

	public static String getClasspathFileURI(String filename) {
		return String.format("%s/%s", SparkTextSearch.class.getClassLoader().getResource(""), filename);
	}

	public static String getClasspathFilePath(String filename) {
		return String.format("%s/%s", SparkTextSearch.class.getClassLoader().getResource("").getPath(), filename);
	}

	public static Properties loadProperties(String propertiesFileName) throws IOException {
		Properties properties = new Properties();
		properties.load(SparkUtils.class.getResourceAsStream(propertiesFileName));
		return properties;
	}

	public static void logPartitionInformation(Logger log, AbstractJavaRDDLike<?, ?> rdd) {
		StringBuilder sb = new StringBuilder();
		for (Partition partition : rdd.partitions()) {
			sb.append(partition.index());
			sb.append("\n");
		}
		log.info(String.format("Partitions #%s: \n%s", rdd.partitions().size(), sb.toString()));
	}
}
