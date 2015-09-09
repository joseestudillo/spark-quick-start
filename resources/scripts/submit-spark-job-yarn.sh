source ./vars.sh
export HADOOP_CONF_DIR=../hadoop/config-cloudera

MASTER=$MASTER_YARN

CMD="
spark-submit \
  --class $CLASS \
  --master $MASTER \
  $JAR \
"


echo $CMD
eval $CMD