#!/usr/bin/env bash

SCALA_VERSION=2.9.3

FWDIR="$(cd `dirname $0`; pwd)"

# Export this as SPARK_HOME
# export DDF_HOME="$FWDIR"

# Find the java binary
if [ -n "${JAVA_HOME}" ]; then
  RUNNER="${JAVA_HOME}/bin/java"
else
  if [ `command -v java` ]; then
    RUNNER="java"
  else
    echo "JAVA_HOME is not set" >&2
  exit 1
  fi
fi

CLASSPATH=`ls -sep ":" $DDF_HOME/spark/target/scala-$SCALA_VERSION/ddf*.jar`
CORE_CLASSPATH=`ls -sep ":" $DDF_HOME/core/target/scala-$SCALA_VERSION/ddf*.jar`
PY4J_CLASSPATH=`ls $DDF_HOME/clients/python/lib/py4j*.jar`
CLASSPATH="$CLASSPATH:$CORE_CLASSPATH:$PY4J_CLASSPATH"
CLASSPATH=${CLASSPATH//[\\n]/:}
echo $CLASSPATH

export CLASSPATH

exec "$RUNNER" -cp "$CLASSPATH" "$@"

