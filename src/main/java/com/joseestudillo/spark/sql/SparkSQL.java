package com.joseestudillo.spark.sql;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.joseestudillo.spark.utils.SparkUtils;

/**
 * Example of general queries in Spark
 * 
 * @author Jose Estudillo
 *
 */
public class SparkSQL {

	private static final Logger log = Logger.getLogger(SparkSQL.class);

	private static final String JSON_TABLE_FILENAME = "table.json";

	private static final String TABLE_NAME = "json_table";
	private static final String FIELD_VALUE = "value";
	private static final String FIELD_ID = "id";

	public static void main(String[] args) {
		SparkConf conf = SparkUtils.getLocalConfig(SparkSQL.class.getSimpleName());
		log.info("access to the web interface at localhost:4040");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(sparkContext);

		DataFrame jsonDataFrame = sqlContext.read().json(SparkUtils.getClasspathFileURI(JSON_TABLE_FILENAME));
		jsonDataFrame.registerTempTable(TABLE_NAME); //this gives a name to the table making it accessible

		// #DataFrame operations
		log.info("Show the whole table");
		jsonDataFrame.show();

		log.info("Show table schema info");
		jsonDataFrame.printSchema();

		log.info("Use as resultSet");
		List<Row> rows = jsonDataFrame.select(FIELD_VALUE).collectAsList();
		for (Row row : rows) {
			log.info(row);
		}

		log.info(String.format("select %s, %s +1", FIELD_VALUE, FIELD_ID));
		jsonDataFrame.select(jsonDataFrame.col(FIELD_VALUE), jsonDataFrame.col(FIELD_ID).plus(1)).show();

		log.info(String.format("filter by %s > 0", FIELD_ID));
		jsonDataFrame.filter(jsonDataFrame.col(FIELD_ID).gt(0)).show();

		log.info(String.format("group by %s", FIELD_VALUE));
		jsonDataFrame.groupBy(FIELD_VALUE).count().show();

		log.info(String.format("group by %s", FIELD_ID));
		jsonDataFrame.groupBy(FIELD_ID).count().show();

		String query = String.format("SELECT * FROM %s", TABLE_NAME);
		DataFrame sqlDataFrame = jsonDataFrame.sqlContext().sql(query);
		log.info(String.format("Using query: %s", query));
		sqlDataFrame.show();

		sparkContext.close();
	}
}
