package com.joseestudillo.spark.sql;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import com.joseestudillo.spark.SparkTextSearch;
import com.joseestudillo.spark.utils.SparkUtils;

/**
 * Example of general operations in Spark SQL
 * 
 * @author Jose Estudillo
 *
 */
public class SparkSQL {

	private static final Logger log = LogManager.getLogger(SparkSQL.class);

	public static void main(String[] args) {
		SparkConf conf = SparkUtils.getLocalConfig(SparkTextSearch.class.getSimpleName());
		log.info("access to the web interface at localhost:4040");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(sparkContext);

		String tableName = "json_table";
		String field0 = "value";
		String field1 = "id";

		DataFrame jsonDataFrame = sqlContext.read().json(SparkUtils.getClasspathFileFullPath("table.json"));
		jsonDataFrame.registerTempTable(tableName); //this gives a name to the table making it accessible

		// #DataFrame operations
		log.info("Show the whole table");
		jsonDataFrame.show();

		log.info("Show table schema info");
		jsonDataFrame.printSchema();

		log.info("Use as resultSet");
		List<Row> rows = jsonDataFrame.select(field0).collectAsList();
		for (Row row : rows) {
			log.info(row);
		}

		log.info("select field1, field2 +1");
		jsonDataFrame.select(jsonDataFrame.col(field0), jsonDataFrame.col(field1).plus(1)).show();

		log.info(String.format("filter by %s > 0", field1));
		jsonDataFrame.filter(jsonDataFrame.col(field1).gt(0)).show();

		log.info(String.format("group by %s", field0));
		jsonDataFrame.groupBy(field0).count().show();

		log.info(String.format("group by %s", field1));
		jsonDataFrame.groupBy(field1).count().show();

		String query = String.format("SELECT * FROM %s", tableName);
		DataFrame sqlDataFrame = jsonDataFrame.sqlContext().sql(query);
		log.info(String.format("Using query: %s", query));
		sqlDataFrame.show();

		String udfName = "strLen";
		log.info(String.format("Declaring the UDF: %s", udfName));
		UDF1<?, ?> udf = (String s) -> s.length();
		sqlContext.udf().register(udfName, udf, DataTypes.IntegerType);

		query = String.format("SELECT %1$s, %2$s(%1$s) FROM %3$s", field0, udfName, tableName);
		DataFrame udfAppliedDataFrame = jsonDataFrame.sqlContext().sql(query);
		log.info(String.format("Using the UDF %s in the query: %s", udfName, query));
		udfAppliedDataFrame.show();

		sparkContext.close();
	}
}
