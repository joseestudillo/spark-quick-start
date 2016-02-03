package com.joseestudillo.spark.sql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
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

import com.joseestudillo.spark.sql.DataFrameSparkSQL.DataBean;
import com.joseestudillo.spark.utils.SparkUtils;

/**
 * Example of loading data into DataFrames from RDDs
 * 
 * @author Jose Estudillo
 *
 */
public class DataFrameSparkSQL {

	private static final Logger log = Logger.getLogger(DataFrameSparkSQL.class);

	private static final String TABLE_NAME = "databean_table";

	//bean to hold the data read from the input RDD. this class must be Serializable.
	public static class DataBean implements Serializable {

		private static final long serialVersionUID = 1L;

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
		SparkConf conf = SparkUtils.getLocalConfig(DataFrameSparkSQL.class.getSimpleName());
		log.info(String.format("access to the web interface at localhost: %s", SparkUtils.SPARK_UI_PORT));
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(sparkContext);

		List<String> inputList = Arrays.asList("abc, 1", "def, 2", "ghi, 3");
		JavaRDD<String> inputRdd = sparkContext.parallelize(inputList);

		// # Read data into a data frame using beans
		org.apache.spark.api.java.function.Function<String, DataBean> fromTextToDataBean = s -> {
			String[] tokens = s.split(",");
			return new DataBean(tokens[0], Long.parseLong(tokens[1].trim()));
		};

		// transform the plain text into DataBeans
		JavaRDD<DataBean> dataBeanRDD = inputRdd.map(fromTextToDataBean);
		log.info(String.format("Transforming the input %s into dataBeans %s", inputRdd.collect(), dataBeanRDD.collect()));

		// Create a DataFrame from the DataBeans RDD
		DataFrame dataBeanDataFrame = sqlContext.createDataFrame(dataBeanRDD, DataBean.class);
		dataBeanDataFrame.registerTempTable(TABLE_NAME);
		log.info(String.format("Table '%s' generated from %s:", TABLE_NAME, dataBeanRDD.collect()));
		dataBeanDataFrame.show();

		// Get all the rows in the dataFrame
		JavaRDD<Row> rddFromDataFrame = dataBeanDataFrame.javaRDD();

		// log the rows in a JSON format
		Function<Row, String> fromRowToString = row -> String.format("Row: {id:%s, name:%s}", row.getLong(0), row.getString(1));
		log.info(String.format("Transforming rowRdd %s into strings: %s", rddFromDataFrame.collect(), rddFromDataFrame.map(fromRowToString).collect()));

		// # Read data into a dataframe using plain data and defining the schema
		// define the column names/types
		List<StructField> columnTypes = new ArrayList<StructField>();
		columnTypes.add(DataTypes.createStructField("id", DataTypes.LongType, true));
		columnTypes.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		StructType tableSchema = DataTypes.createStructType(columnTypes);

		log.info(String.format("Table Squema: %s", tableSchema));

		// Create rows (Row) from plain text
		Function<String, Row> fromStringToRow = inputText -> {
			String[] fields = inputText.split(",");
			return (Row) RowFactory.create(fields[1].trim(), fields[0]);
		};
		JavaRDD<Row> rowsFromDataRDD = inputRdd.map(fromStringToRow);
		log.info(String.format("inputRdd %s -> outputRowRdd %s", inputRdd.collect(), rowsFromDataRDD.collect()));

		// Apply the schema to the RDD and load the Rows from the RDD.
		DataFrame definedSchemaDataFrame = sqlContext.createDataFrame(rowsFromDataRDD, tableSchema);
		definedSchemaDataFrame.registerTempTable(TABLE_NAME);
		log.info(String.format("Table '%s' generated from %s:", TABLE_NAME, rowsFromDataRDD.collect()));
		definedSchemaDataFrame.show();

	}
}
