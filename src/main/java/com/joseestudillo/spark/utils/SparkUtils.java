package com.joseestudillo.spark.utils;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.spark.SparkConf;

import com.joseestudillo.spark.SparkTextSearch;

import scala.Tuple2;

public class SparkUtils {

	private static final String LOCAL_MASTER_ID = "local[*]";

	public static SparkConf getLocalConfig(String appName) {
		return new SparkConf().setAppName(appName).setMaster(LOCAL_MASTER_ID);
	}

	public static void listProperties(SparkConf conf, PrintWriter pw) {
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
			return "Unknown";
		}
	}

	public static String getClasspathFileFullPath(String filename) {
		return String.format("%s/%s", SparkTextSearch.class.getClassLoader().getResource(""), filename);
	}

	public static Properties loadProperties(String propertiesFileName) throws IOException {
		Properties properties = new Properties();
		properties.load(SparkUtils.class.getResourceAsStream(propertiesFileName));
		return properties;
	}
}
