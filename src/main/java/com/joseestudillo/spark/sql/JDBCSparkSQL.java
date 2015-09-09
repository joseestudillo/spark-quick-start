package com.joseestudillo.spark.sql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.joseestudillo.spark.utils.DerbyManager;
import com.joseestudillo.spark.utils.SparkUtils;

/**
 * Example of how to load a table using JDBC into a {@code DataFrame}
 * 
 * @author Jose Estudillo
 *
 */
public class JDBCSparkSQL {

	private static final Logger log = Logger.getLogger(JDBCSparkSQL.class);

	private static final String CREATE_TMP_TABLE_QUERY = "CREATE TEMPORARY TABLE jdbc_table USING org.apache.spark.sql.jdbc OPTIONS ( url \"%s\", dbtable \"%s\")";
	private static final String DATABASE_NAME = "spark_database";
	private static final String TABLE_NAME = DerbyManager.DERBY_TABLE_NAME;

	private static final String SPARK_JDBC = "jdbc";
	private static final String SPARK_JDBC_OPT_URL = "url";
	private static final String SPARK_JDBC_OPT_DBTABLE = "dbtable";

	public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		SparkConf conf = SparkUtils.getLocalConfig(JDBCSparkSQL.class.getSimpleName());
		log.info(String.format("access to the web interface at localhost: %s", SparkUtils.SPARK_UI_PORT));
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		String connStr = DerbyManager.getConnectionString(DATABASE_NAME);

		DerbyManager.loadDriver();
		Connection conn = DerbyManager.getDerbyConnection(DATABASE_NAME);

		//create a table to load later on in Spark
		DerbyManager.createDummyTable(conn);

		//show the content of the just created table
		log.info(String.format("Content in the table '%s':", TABLE_NAME));
		DerbyManager.logDummyTableContent(conn, log);

		// load the table into spark
		// this way to load the table appears in the latest version of the documentation (1.4) but it is marked as deprecated 
		SQLContext sqlContext = new SQLContext(sparkContext);
		Map<String, String> options = new HashMap<String, String>();
		options.put(SPARK_JDBC_OPT_URL, connStr);
		options.put(SPARK_JDBC_OPT_DBTABLE, TABLE_NAME);
		log.info(String.format("Loading table %s from %s with the options: %s", TABLE_NAME, connStr, options));
		DataFrame jdbcDataFrame = sqlContext.load(SPARK_JDBC, options);
		jdbcDataFrame.show();

		//TODO this way doesn't work, probably compatibility problems with Derby, it should work with PostgreSQL/MySQL
		//		String createDatabaseQuery = String.format(CREATE_TMP_TABLE_QUERY, connStr, TABLE_NAME);
		//		log.info(String.format("Creating table %s using the query: %s", TABLE_NAME, createDatabaseQuery));
		//		jdbcDataFrame = sqlContext.sql(createDatabaseQuery);
		//		jdbcDataFrame.show();

		conn.close();
		sparkContext.close();
	}
}
