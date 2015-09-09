source ./vars.sh
# if the hostname is not the same as the one that appears in the web interface it doesn't work
MASTER=$MASTER_LOCAL

CMD="
spark-submit \
  --class $CLASS \
  --master $MASTER \
  $JAR \
"
echo $CMD
eval $CMD