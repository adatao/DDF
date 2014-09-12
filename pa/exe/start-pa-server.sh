#!/bin/bash

usage() {
    echo "
    Usage: start-pa-server
        [--cluster (default: mesos; other options is:
                            yarn (for yarn cluster),
                            spark (for distributed standalone spark),
                            localspark (for local single-node standalone spark)]
        [--start-spark (default: No)]
    "
    exit 1
}

[[ "$1" == -h || "$1" == --help ]] && usage

cluster=mesos

do_parse_args() {
    while [[ -n "$1" ]] ; do
        case $1 in
                "--cluster" )
                    shift ; cluster=$1 ; shift
                    ;;

                "--start-spark" )
                    shift ; start_spark=1
                    ;;

                * )
                    usage "Unknown switch '$1'"
                    ;;
        esac
    done
}
do_parse_args $@

cd `dirname $0`/../ >/dev/null 2>&1
DIR=`pwd`

if [[ -z "$SPARK_MEMORY" ]]; then
	. ${DIR}/exe/mem-size-detection.sh
fi
echo "SPARK_MEMORY = "$SPARK_MEMORY

paenv="${DIR}/conf/pa-env.sh"
[ ! -f $paenv ] && echo "Fatal: $paenv file does not exist" && exit 1

echo
echo "#################################################"
echo "# Export pAnalytics/Spark Environment Variables #"
echo "#################################################"
echo
source $paenv $@ --cluster $cluster
echo "SPARK_MASTER="${SPARK_MASTER}
echo "SPARK_CLASSPATH="${SPARK_CLASSPATH}
echo "SPARK_JAVA_OPTS="${SPARK_JAVA_OPTS}

mkdir -p ${LOG_DIR}

${DIR}/exe/stop-pa-server.sh


[ "X$start_spark" == "X1" ] && ${DIR}/exe/start-spark-cluster.sh $@

# Start Rserve
#${DIR}/exe/start-rserve.sh

echo
echo "###########################"
echo "# Start pAnalytics server #"
echo "###########################"
#nohup
${DIR}/exe/spark-class -Dpa.security=false -Dbigr.multiuser=false -Dlog.dir=${LOG_DIR} com.adatao.pa.thrift.Server $PA_PORT #>${LOG_DIR}/pa.out 2>&1 &
#${DIR}/exe/bin/spark-submit --class com.adatao.pa.thrift.Server --master ${SPARK_MASTER} --num-executors ${SPARK_WORKER_INSTANCES} --driver-memory ${SPARK_MEM} --executor-memory ${SPARK_MEM} --executor-cores ${SPARK_WORKER_CORES} ${DIR}/target/scala-2.10/ddf_pa-assembly-1.0.jar  #>${LOG_DIR}/pa.out 
echo

sleep 5
pgrep -fl com.adatao.pa.thrift.Server >/dev/null 2>&1 || echo "Error: No 'Server' Java process found. Something is wrong"

