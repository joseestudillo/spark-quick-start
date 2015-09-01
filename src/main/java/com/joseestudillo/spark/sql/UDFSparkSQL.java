package com.joseestudillo.spark.sql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import com.joseestudillo.spark.SparkTextSearch;
import com.joseestudillo.spark.utils.SparkUtils;

/**
 * Example UDF creation and use in Spark
 * 
 * @author Jose Estudillo
 *
 */
public class UDFSparkSQL {

	private static final Logger log = LogManager.getLogger(UDFSparkSQL.class);

	private static final String JSON_TABLE_FILENAME = "table.json";

	private static final String TABLE_NAME = "json_table";
	private static final String FIELD_VALUE = "value";
	private static final String FIELD_ID = "id";

	private static final String UDF_NAME = "strLen";

	public static void main(String[] args) {
		SparkConf conf = SparkUtils.getLocalConfig(SparkTextSearch.class.getSimpleName());
		log.info("access to the web interface at localhost:4040");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(sparkContext);

		DataFrame jsonDataFrame = sqlContext.read().json(SparkUtils.getClasspathFileURI(JSON_TABLE_FILENAME));
		jsonDataFrame.registerTempTable(TABLE_NAME); //this gives a name to the table making it accessible

		// #DataFrame operations
		log.info("Show the whole table");
		jsonDataFrame.show();

		log.info(String.format("Declaring the UDF: %s", UDF_NAME));
		UDF1<?, ?> udf = (String s) -> s.length();
		sqlContext.udf().register(UDF_NAME, udf, DataTypes.IntegerType);

		String query = String.format("SELECT %1$s, %2$s(%1$s) FROM %3$s", FIELD_VALUE, UDF_NAME, TABLE_NAME);
		DataFrame udfAppliedDataFrame = jsonDataFrame.sqlContext().sql(query);
		log.info(String.format("Using the UDF %s in the query: %s", UDF_NAME, query));
		udfAppliedDataFrame.show();

		sparkContext.close();
	}
}
