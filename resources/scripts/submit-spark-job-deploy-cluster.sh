source ./vars.sh
# if the hostname is not the same as the one that appears in the web interface it doesn't work
MASTER=$MASTER_SPARK
DEPLOY_MODE=cluster

CMD="
spark-submit \
  --deploy-mode $DEPLOY_MODE \
  --class $CLASS \
  --master $MASTER \
  $JAR \
"
echo $CMD
eval $CMD