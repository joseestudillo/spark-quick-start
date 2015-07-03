package com.joseestudillo.spark.utils;

import org.apache.spark.SparkConf;

public class SparkUtils {
	
	private static final String LOCAL_MASTER_ID = "local";
	public static SparkConf getLocalConfig(String appName) {
		return new SparkConf().setAppName(appName).setMaster(LOCAL_MASTER_ID);
	}
}
