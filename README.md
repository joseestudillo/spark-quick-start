# Spark

Spark quick start introduction

# Concepts

- _RDD (resilient distributed datasets)_: it represents how the information is stored in Spark. RDD are immutable, every operation always generates a new RDD. The path of operations that leads you to a result is known as _directed acyclic graph (DAG)_.
  - RDD types:
    - RDD `<T>` 
    - Numeric RDD
    - Pairs RDD

  - Operation types:
  	- Transformations: are operations over an RDD that doesn't imply the generation of a result. Every transformation results in a new RDD. An example of this can be filtering an _RDD_. They are evaluated lazily, so these operations are only executed when an _action_ is called. 
  	- Actions: An operations that leads into the execution of the plan to generate a result. An example of this can be collecting the elemements in an RDD.
- _Driver Program_: Main program, connect to the resource administrator and allocate workers.
- _Worker_: A machine in the cluster assigned to execute task.
- _Executor_: Are launched by the driver and they remain until the work in done. They do two things, they run tasks to return result to the driver, and they provide in-memory storage for RDDs that are cached by user programs.
- _Task_: Unit of work to be done.


# Installation

- create the folder `/user/local/apache/spark/`
- extract the content of the latest release into it. At the time of this writing `spark-1.4.0-bin-hadoop2.6`
- create a symb link: `ln -sf spark-1.4.0-bin-hadoop2.6/ current`
- create the environment variables:
```bash
 #Spark
export SPARK_HOME=/usr/local/apache/spark/current
export PATH=$PATH:$SPARK_HOME/bin
```

# CLI Tools

- `bin/spark-shell`: Scala shell for Spark.

- `bin/pyspark`: Python shell for Spark.

- `bin/spark-submit`: Allow to launch an spark application with fault tolerant in the driver, being able to launch the driver if it fails, this is specially useful for streaming applications. This is also used to launch spark python applications.

- `/bin/spark-sql`: SQL shell for Spark.

# Code examples

## RDDs
	
- operations with RDDs: `com.joseestudillo.spark.rdd.RddTransAndOps`
- Numeric RDDs: `com.joseestudillo.spark.rdd.DoubleRDDs`
- Pair RDDs: `com.joseestudillo.spark.rdd.PairRDDs`

### Partitioning

- `com.joseestudillo.spark.rdd.Partitioning`

### Persistence

- `com.joseestudillo.spark.rdd.Persistence`

### More Operations

- Piping: `com.joseestudillo.spark.rdd.Piping`

## Accumulators

- `com.joseestudillo.spark.rdd.Accumulators`

## Broadcast Variables

- `com.joseestudillo.spark.rdd.BroadcastVariables`


# Spark SQL

- Basic Spark SQL operations: `com.joseestudillo.spark.sql.SparkSQL`
- Using JDBC with Spark SQL: `com.joseestudillo.spark.sql.JDBCSparkSQL`
- Creating DataFrames from RDDs in Spark SQL: `com.joseestudillo.spark.sql.DataFrameSparkSQL`
- Loading Saving data in SparkSQL: `com.joseestudillo.spark.sql.StorageSparkSQL`
- Hive integration in SparkSQL: `com.joseestudillo.spark.sql.HiveSparkSQL`
- Creating and using UDF in Spark: `com.joseestudillo.spark.sql.UDFSparkSQL`
- Memory management for DataFrames and tables in Spark: `com.joseestudillo.spark.sql.MemoryManagementSparkSQL`

# Spark Streaming

- General Spark Stream operations: `com.joseestudillo.spark.stream.SparkStreaming`

# Submitting task to an Spark Cluster

- The IP/Hostname used  to refer the spark cluster must the be the same in the hostname and in the cluster (it doesn't matter if they resolve to the same, they must be the same)

