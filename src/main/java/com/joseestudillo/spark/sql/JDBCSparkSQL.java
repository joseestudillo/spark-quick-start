package com.joseestudillo.spark.sql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.joseestudillo.spark.SparkTextSearch;
import com.joseestudillo.spark.utils.DerbyManager;
import com.joseestudillo.spark.utils.SparkUtils;

/**
 * 
 * @author Jose Estudillo
 *
 */
public class JDBCSparkSQL {

	private static final Logger log = LogManager.getLogger(JDBCSparkSQL.class);

	private static String CREATE_TMP_TABLE_QUERY = "CREATE TEMPORARY TABLE jdbc_table USING org.apache.spark.sql.jdbc OPTIONS ( url \"%s\", dbtable \"%s\")";

	public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		SparkConf conf = SparkUtils.getLocalConfig(SparkTextSearch.class.getSimpleName());
		log.info("access to the web interface at localhost:4040");
		JavaSparkContext spark = new JavaSparkContext(conf);

		String databaseName = "spark_database";
		String connStr = DerbyManager.getConnectionString(databaseName);
		String dbtable = DerbyManager.DERBY_TABLE_NAME;//String.format("%s.%s", databaseName, DerbyManager.DERBY_TABLE_NAME);

		DerbyManager.loadDriver();
		Connection conn = DerbyManager.getDerbyConnection(databaseName);
		DerbyManager.createDummyTable(conn);

		log.info(String.format("Content in the table '%s':", dbtable));
		DerbyManager.logDummyTableContent(conn, log);

		SQLContext sqlContext = new SQLContext(spark);

		// this appears in the latest version of the documentation (1.4) but it is mark as deprecated
		Map<String, String> options = new HashMap<String, String>();
		options.put("url", connStr);
		options.put("dbtable", dbtable);
		log.info(String.format("Loading table %s from %s with the options: %s", dbtable, connStr, options));
		DataFrame jdbcDataFrame = sqlContext.load("jdbc", options);
		jdbcDataFrame.show();

		String createDatabaseQuery = String.format(CREATE_TMP_TABLE_QUERY, connStr, dbtable);
		log.info(String.format("Loading table %s using the query: %s", dbtable, createDatabaseQuery));
		jdbcDataFrame = sqlContext.sql(createDatabaseQuery); //TODO this way doesn't work, probably problems with the query
		jdbcDataFrame.show();

		conn.close();
		spark.close();
	}
}
