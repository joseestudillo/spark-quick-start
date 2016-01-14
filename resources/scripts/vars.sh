CLASS=com.joseestudillo.spark.SparkBasicDriver
JAR=`ls ../../target/spark-*.jar | head -1`
CLUSTER_HOSTNAME=sandbox
MASTER_LOCAL=local[*]
# if the hostname is not the same as the one that appears in the web interface it doesn't work
MASTER_SPARK=spark://$CLUSTER_HOSTNAME:7077
MASTER_YARN=yarn://$CLUSTER_HOSTNAME:8042