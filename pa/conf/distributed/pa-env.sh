#!/bin/bash
#
# Put environment-specific overrides here. conf/pa-env.sh will source this last
#
echo ""
echo "######## Running In DISTRIBUTED Mode ########"
echo ""

export LOG_DIR="/mnt/log/"
echo "LOG_DIR="${LOG_DIR}

export TMP_DIR="/mnt/tmp"
echo "TMP_DIR="${TMP_DIR}

export SPARK_MASTER=`cat /root/spark-ec2/cluster-url`
echo "SPARK_MASTER="${SPARK_MASTER}

export MESOS_NATIVE_LIBRARY=/usr/local/lib/libmesos-0.13.0.so
echo "MESOS_NATIVE_LIBRARY"=${MESOS_NATIVE_LIBRARY}

export HADOOP_HOME=/root/ephemeral-hdfs
echo "HADOOP_HOME="${HADOOP_HOME}

export HIVE_HOME=/root/hive-0.9.0-bin
echo "HIVE_HOME="${HIVE_HOME}

# set crash log location
SPARK_JAVA_OPTS+=" -XX:ErrorFile=/mnt/log/bigr-crash-%p.log"

# set spark directories on /mnt*
source /root/spark-ec2/ec2-variables.sh
export SPARK_JAVA_OPTS+=" -Dspark.local.dir=${MESOS_SPARK_LOCAL_DIRS}"

#export SPARK_JAVA_OPTS+=" -Dspark.local.dir=/mnt/spark,/mnt2/spark,/mnt3/spark,/mnt4/spark"
#export SPARK_MEM=12288m
