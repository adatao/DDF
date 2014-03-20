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
		[--standalone-spark (default: No)]
	"
	exit 1
}
[[ "$1" == -h || "$1" == --help ]] && usage

do_parse_args() {
	while [[ -n "$1" ]] ; do
		case $1 in
			"--standalone-spark" )
				shift ; standalone_spark=1
				;;
		esac
		shift
	done
}
do_parse_args $@

define PA_HOME "$(cd `dirname ${BASH_SOURCE[0]}`/../ >/dev/null 2>&1; echo $PWD)"
define CONF_DIR "${PA_HOME}/conf"
define LOG_DIR "/tmp"
define TMP_DIR "/tmp"
define SPARK_HOME "${PA_HOME}/exe"
define RLIBS "${PA_HOME}/rlibs" 
define RSERVE_LIB_DIR "${RLIBS}/Rserve/libs/"
define RSERVER_JAR "`find ${PA_HOME}/target/scala-*/bigr-server_*.jar | grep -v tests`"
define PA_PORT 7911
SPARK_CLASSPATH=$RSERVER_JAR
SPARK_CLASSPATH+=:"${PA_HOME}/../lib_managed/jars/*"
SPARK_CLASSPATH+=:"${PA_HOME}/../lib_managed/bundles/*"
SPARK_CLASSPATH+=:"${PA_HOME}/../lib_managed/orbits/*"
SPARK_CLASSPATH+=:"${PA_HOME}/conf/"
[ "X$HIVE_HOME" != "X" ] && SPARK_CLASSPATH+=":${HIVE_HOME}/conf/"
[ "X$HADOOP_HOME" != "X" ] && SPARK_CLASSPATH+=":${HADOOP_HOME}/conf/"
export SPARK_CLASSPATH
SPARK_JAVA_OPTS="-Dspark.storage.memoryFraction=0.4"
SPARK_JAVA_OPTS+=" -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
SPARK_JAVA_OPTS+=" -Dspark.serializer=org.apache.spark.serializer.KryoSerializer -Dspark.kryo.registrator=adatao.bigr.spark.KryoRegistrator"
SPARK_JAVA_OPTS+=" -Dlog4j.configuration=pa-log4j.properties"
SPARK_JAVA_OPTS+=" -Djava.io.tmpdir=${TMP_DIR}"
define SPARK_JAVA_OPTS "$SPARK_JAVA_OPTS"

#export HADOOP_HOME 
#export HIVE_HOME
#export SPARK_MASTER # export this if you don't use standalone spark
define SERVER_MODE "`source $CONF_DIR/determine-server-mode.sh $@`"	
[ -f "$CONF_DIR/$SERVER_MODE/pa-env.sh" ] && source "$CONF_DIR/$SERVER_MODE/pa-env.sh" ]
[ "X$HADOOP_HOME" != "X" ] && SPARK_CLASSPATH+=":${HADOOP_HOME}/conf/"
[ "X$HIVE_HOME" != "X" ] && SPARK_CLASSPATH+=":${HIVE_HOME}/conf/"
SPARK_CLASSPATH+=":${CONF_DIR}/${SERVER_MODE}"
define SPARK_CLASSPATH "$SPARK_CLASSPATH"

if [ "X$standalone_spark" == "X1" ]; then
	export SPARK_HOST=localhost
	export SPARK_PORT=7070
	export SPARK_MASTER=spark://${SPARK_HOST}:${SPARK_PORT}	
else
	[ "X$SPARK_MASTER" == "X" ] && echo "Since you are NOT running pAnalytics with stand-alone Spark, please define export SPARK_MASTER" && exit 1
fi
