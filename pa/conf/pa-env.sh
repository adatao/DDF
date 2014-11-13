#!/bin/bash -

#
# Define and export an environment variable
#
# @param $1 name
# @param $2 value - if empty, then variable is unset
# @return side effect of setting the variable
#
function define {
  if [ "$2" != "" ] ; then
    export $1="$2"
    echo "$1=$2"
  else
    echo "Unsetting $1"
    unset $1
  fi
}

usage() {
        echo "
        Usage: pa-env.sh
            [--cluster (default: mesos; other options is:
            yarn (for yarn cluster),
            spark (for distributed standalone spark),
            localspark (for local single-node standalone spark)]
        "
        exit 1
}
[[ "$1" == -h || "$1" == --help ]] && usage

cluster=mesos
do_parse_args() {
        while [[ -n "$1" ]] ; do
                case $1 in
                        "--cluster" )
                                shift ; cluster=$1
                                ;;
                esac
                shift
        done
}
do_parse_args $@

export PA_HOME="$(cd `dirname ${BASH_SOURCE[0]}`/../ >/dev/null 2>&1; echo $PWD)"
#######################################################
# You need to define the following evn vars           #
#######################################################
export TMP_DIR=/tmp # this where pAnalytics server stores temporarily files
export LOG_DIR=/tmp # this where pAnalytics server stores log files
export SPARK_HOME=${PA_HOME}/exe/
export PA_PORT=7911
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/root/hadoop-2.2.0.2.0.6.0-101/conf}

export RLIBS="${PA_HOME}/rlibs"
export RSERVE_LIB_DIR="${RLIBS}/Rserve/libs/"
export RSERVER_JAR=`find ${PA_HOME}/ -name ddf_pa_*.jar | grep -v '\-tests.jar'`
export DDFSPARK_JAR=`find ${PA_HOME}/../spark_adatao/ -name ddf_spark_adatao-assembly*.jar | grep -v '\-tests.jar'`
echo RSERVER_JAR=$RSERVER_JAR
echo DDFSPARK_JAR=$DDFSPARK_JAR
SPARK_CLASSPATH=$RSERVER_JAR

SPARK_CLASSPATH+=:"$DDFSPARK_JAR"
SPARK_CLASSPATH+=:"${PA_HOME}/../lib_managed/jars/*"
SPARK_CLASSPATH+=:"${PA_HOME}/../lib_managed/bundles/*"
SPARK_CLASSPATH+=:"${PA_HOME}/../lib_managed/orbits/*"
SPARK_CLASSPATH+=:"${PA_HOME}/conf/distributed/"

#The order of the following two lines is important please dont change
SPARK_CLASSPATH+=":${HIVE_CONF_DIR}"
[ "X$HADOOP_CONF_DIR" != "X" ] && SPARK_CLASSPATH+=":${HADOOP_CONF_DIR}"
export SPARK_CLASSPATH

SPARK_JAVA_OPTS="-Dspark.storage.memoryFraction=0.6"
SPARK_JAVA_OPTS+=" -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
SPARK_JAVA_OPTS+=" -Dspark.serializer=org.apache.spark.serializer.KryoSerializer -Dspark.kryo.registrator=io.spark.content.KryoRegistrator"
SPARK_JAVA_OPTS+=" -Dlog4j.configuration=pa-log4j.properties"
SPARK_JAVA_OPTS+=" -Dspark.local.dir=${TMP_DIR}"
SPARK_JAVA_OPTS+=" -Dspark.ui.port=30001"
SPARK_JAVA_OPTS+=" -Djava.io.tmpdir=${TMP_DIR}"
SPARK_JAVA_OPTS+=" -Dspark.kryoserializer.buffer.mb=125"
SPARK_JAVA_OPTS+=" -Dspark.executor.memory=${SPARK_MEMORY}"
SPARK_JAVA_OPTS+=" -Dspark.driver.memory=${SPARK_MEMORY}"
SPARK_JAVA_OPTS+=" -Dspark.sql.inMemoryColumnarStorage.compressed=true"
SPARK_JAVA_OPTS+=" -Dspark.sql.inMemoryColumnarStorage.batchSize=1000000"
SPARK_JAVA_OPTS+=" -Dspark.akka.heartbeat.interval=3"
SPARK_JAVA_OPTS+=" -Dbigr.Rserve.split=1"
SPARK_JAVA_OPTS+=" -Dbigr.multiuser=false"
SPARK_JAVA_OPTS+=" -Dspark.shuffle.manager=sort"
SPARK_JAVA_OPTS+=" -Dspark.worker.reconnect.interval=10"
#SPARK_JAVA_OPTS+=" -Dpa.keytab.file=${PA_HOME}/conf/pa.keytabs"
#SPARK_JAVA_OPTS+=" -Dpa.authentication=true"
#SPARK_JAVA_OPTS+=" -Dpa.admin.user=pa"
#SPARK_JAVA_OPTS+=" -Drun.as.admin=true"
#SPARK_JAVA_OPTS+=" -Dsun.security.krb5.debug=true"
export SPARK_JAVA_OPTS
if [ "X$cluster" == "Xyarn" ]; then
        echo "Running pAnalytics with Yarn"
        export SPARK_MASTER="yarn-client"
        export SPARK_WORKER_INSTANCES=12
        export SPARK_WORKER_CORES=8
        export SPARK_WORKER_MEMORY=$SPARK_MEMORY
        export SPARK_DRIVER_MEMORY=$SPARK_MEMORY
        export SPARK_JAR=`find ${PA_HOME}/ -name ddf_pa-assembly-*.jar`
        echo SPARK_JAR=$SPARK_JAR
        export SPARK_YARN_APP_JAR=hdfs:///user/root/ddf_pa_2.10-1.2.0.jar
        
        SPARK_CLASSPATH+=:"${PA_HOME}/conf/distributed/"
         
        #export SPARK_JAR=`find ${PA_HOME}/ -name ddf_pa-assembly-*.jar`
        #export SPARK_YARN_APP_JAR=hdfs:///user/root/ddf_pa-assembly-0.9.jar
        [ "X$SPARK_YARN_APP_JAR" == "X" ] && echo "Please define SPARK_YARN_APP_JAR" && exit 1
        [ "X$HADOOP_CONF_DIR" == "X" ] && echo "Please define HADOOP_CONF_DIR" && exit 1
        [ "X$SPARK_WORKER_INSTANCES" == "X" ] && echo "Notice! SPARK_WORKER_INSTANCES is not defined, the default value will be used instead"
        [ "X$SPARK_WORKER_CORES" == "X" ] && echo "Notice! SPARK_WORKER_CORES is not defined, the default value will be used instead"
elif [ "X$cluster" == "Xmesos" ]; then
        echo "Running pAnalytics with Mesos"
        #export SPARK_MASTER= #mesos://<host>:<port>
elif [ "X$cluster" == "Xspark" ]; then
        echo "Running pAnalytics with Spark"
        #export SPARK_MASTER= #spark://<host>:<port>
elif [ "X$cluster" == "Xlocalspark" ]; then
        echo "Running pAnalytics with Spark in local node"
        export SPARK_MEM=$SPARK_MEMORY
       # export SPARK_WORKER_MEMORY=$SPARK_MEMORY
        export SPARK_MASTER=local
        SPARK_JAVA_OPTS+=" -Dlog4j.configuration=pa-local-log4j.properties" 
        #SPARK_CLASSPATH+=:"pa-local-log4j.properties"
        SPARK_CLASSPATH+=:"${PA_HOME}/conf/local/"
fi
export SPARK_CLASSPATH
