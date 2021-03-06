#to run a jar file from a computer that is not part of the spark cluster

source ./vars.sh
# if the hostname is not the same as the one that appears in the web interface it doesn't work
MASTER=$MASTER_SPARK
DEPLOY_MODE=client

CMD="
spark-submit \
  --deploy-mode $DEPLOY_MODE \
  --class $CLASS \
  --master $MASTER \
  $JAR \
"
echo $CMD
eval $CMD