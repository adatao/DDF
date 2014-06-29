#!/bin/bash

#########################################
###to build ddf project##################
###and put assembly *.jar file to hdfs###
#########################################

export SPARK_HOME="$(cd `dirname ${BASH_SOURCE[0]}`/../ >/dev/null 2>&1; echo $PWD)"
export DDF_HOME=${SPARK_HOME}/../

echo SPARK_HOME=$SPARK_HOME
echo DDF_HOME=$DDF_HOME
echo "# running bin/sbt clean compile package #"
cd $DDF_HOME
bin/sbt clean compile package

echo "# assembly pa project #"
bin/sbt 'project spark' assembly

echo "# copy jars to slaves, and put assembly fat jar to hdfs #"

/root/spark-ec2/copy-dir.sh $DDF_HOME &
${HADOOP_HOME}/bin/hdfs dfs -rmr /user/root/ddf_spark-assembly-0.9.jar

echo "# put assembly fat jar to hdfs #"
${HADOOP_HOME}/bin/hdfs dfs -put ${SPARK_HOME}/target/scala-2.10/ddf_spark-assembly-0.9.jar /user/root
wait
echo "# THANK YOU, DONE #"

