package com.joseestudillo.spark.sql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.joseestudillo.spark.SparkTextSearch;
import com.joseestudillo.spark.utils.SparkUtils;

public class RDDSparkSQL {

	private static final Logger log = LogManager.getLogger(RDDSparkSQL.class);

	//bean to hold the data read from the input RDD. this class must be serializable.
	public static class DataBean implements Serializable {
		private String name;
		private long id;

		public DataBean(String name, long id) {
			this.name = name;
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		@Override
		public String toString() {
			return "DataBean [name=" + name + ", id=" + id + "]";
		}
	}

	public static void main(String[] args) {
		SparkConf conf = SparkUtils.getLocalConfig(SparkTextSearch.class.getSimpleName());
		log.info("access to the web interface at localhost:4040");
		JavaSparkContext spark = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(spark);

		List<String> inputList = Arrays.asList("abc, 1", "def, 2", "ghi, 3");
		JavaRDD<String> inputRdd = spark.parallelize(inputList);

		// #Read data using beans (data => bean =using spark lib + class definition=> table)
		org.apache.spark.api.java.function.Function<String, DataBean> parseDataBean = s -> {
			String[] tokens = s.split(",");
			return new DataBean(tokens[0], Long.parseLong(tokens[1].trim()));
		};

		JavaRDD<DataBean> dataBeanRdd = inputRdd.map(parseDataBean);
		log.info(String.format("Transforming the input %s into %s", inputRdd.collect(), dataBeanRdd.collect()));

		DataFrame schemaData = sqlContext.createDataFrame(dataBeanRdd, DataBean.class);
		String tableName = "dataBeanTable";
		schemaData.registerTempTable(tableName);
		log.info(String.format("Table '%s' generated from %s:", tableName, dataBeanRdd.collect()));
		schemaData.show();

		JavaRDD<Row> rddFromDataFrame = schemaData.javaRDD();
		Function<Row, String> fromRowToString = row -> String.format("Row: {id:%s, name:%s}", row.getLong(0), row.getString(1));
		log.info(String.format("Transforming rowRdd %s into strings: %s", rddFromDataFrame.collect(), rddFromDataFrame.map(fromRowToString).collect()));

		// #Read data using plain data and defining the schema (data => row =using squema=> table)
		//define the column names/types
		List<StructField> columnTypes = new ArrayList<StructField>();
		columnTypes.add(DataTypes.createStructField("id", DataTypes.LongType, true));
		columnTypes.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		StructType tableSchema = DataTypes.createStructType(columnTypes);

		log.info(String.format("Table Squema: %s", tableSchema));

		Function<String, Row> fromStringToRow = record -> {
			String[] fields = record.split(",");
			return (Row) RowFactory.create(fields[1].trim(), fields[0]);
		};
		JavaRDD<Row> rowsFromDataRdd = inputRdd.map(fromStringToRow);
		log.info(String.format("inputRdd %s => outputRowRdd %s", inputRdd.collect(), rowsFromDataRdd.collect()));

		// Apply the schema to the RDD.
		DataFrame definedSchemaDataFrame = sqlContext.createDataFrame(rowsFromDataRdd, tableSchema);
		definedSchemaDataFrame.registerTempTable(tableName);
		log.info(String.format("Table '%s' generated from %s:", tableName, rowsFromDataRdd.collect()));
		definedSchemaDataFrame.show();

	}
}
