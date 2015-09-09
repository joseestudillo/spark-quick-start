package com.joseestudillo.spark.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * Utility class for Log4j
 * 
 * @author Jose Estudillo
 *
 */

public class LoggerUtils {

	/**
	 * Logs the content of a given file
	 * 
	 * @param log
	 * @param file
	 * @throws IOException
	 */
	public static void logFileContent(Logger log, File file) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line;
		StringBuffer sb = new StringBuffer();
		while ((line = br.readLine()) != null) {
			sb.append(line);
			sb.append("\n");
		}
		br.close();
		log.info(String.format("%s: \n%s", file, sb.toString()));
	}
}
