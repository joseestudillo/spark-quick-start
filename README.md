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

- The IP/Hostname used  to refer the spark cluster must the be the same in the hostname and in the cluster (it doesn't matter if they resolve to the same, they must be the same). To avoid this kind of problems declare explicitly the host ip and the hostname in the `/etc/hosts` file.

- It is also required to run the same version of Spark as in the cluster, the submit command is not guaranteed to work with different versions.

- _Deploy modes:_ 
  - _Client mode_: when the computer that contains the app wants to run it on an spark cluster. 
  - _Cluster mode_: the same as above, but the host computer is park of the cluster itself, so spark expect to access to the files locally. 

## Submitting Spark jobs into a Cloudera Quick Start VM

At the time of this writing I'm using the version 5.4 and the only defined worker fails because it can't connect to Akka. The easiest way to solve this is to configure CentOS to use an static IP and then configure that IP in the `/etc/hosts` file. Because of the cloudera vm is configured to generate `/etc/hosts` on startup you will need to do the following:

- In the file `/etc/init.d/cloudera-quickstart-init` comment out the line that executes the command `cloudera-quickstart-ip`
- Then edit the `/etc/hosts` file the following:
```bash
127.0.0.1	quickstart	localhost	localhost.domain
YOUR_STATIC_IP     quickstart.cloudera
```

In this case I'm using `quickstart.cloudera` as a main hostname, so I will also configure it in the host machine to be able to submit jobs remotely to the VM.

After this change you can check that the worker now can properly register to Akka in the log file `/var/log/spark/spark-worker.out`.