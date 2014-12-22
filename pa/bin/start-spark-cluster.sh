#!/bin/bash

echo
echo "#############################" 
echo "# Start local Spark cluster #"
echo "#############################"

DIR="$(cd `dirname $0`/../ 2>&1 >/dev/null; echo $PWD)"
paenv="$DIR/conf/pa-env.sh" ; source $paenv --standalone-spark

${DIR}/exe/stop-spark-cluster.sh
nohup ${DIR}/exe/spark-class \
    org.apache.spark.deploy.master.Master \
    --ip ${SPARK_HOST} \
    --port ${SPARK_PORT} \
    &>${TMP_DIR}/spark-master.out </dev/null &
nohup ${DIR}/exe/spark-class \
    org.apache.spark.deploy.worker.Worker \
    ${SPARK_MASTER} &>${TMP_DIR}/spark-worker.out </dev/null &

sleep 10

pgrep -f org.apache.spark.deploy.master.Master >/dev/null 2>&1 || echo "Error: No 'Master' Java process found. Something is wrong"
pgrep -f org.apache.spark.deploy.worker.Worker >/dev/null 2>&1 || echo "Error: No 'Worker' Java process found. Something is wrong"

