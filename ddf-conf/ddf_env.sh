#!/usr/bin/env bash
#
# This file should only contain simple environment variable assignments
#
export SCALA_VERSION=2.9.3
export HADOOP_HOME=${HADOOP_HOME-/root/hadoop}
export HIVE_HOME=${HIVE_HOME-/root/hive}

export JAVA_OPTS+="-Dderby.stream.error.file=/tmp/hive/derby.log"

echo $JAVA_OPTS
