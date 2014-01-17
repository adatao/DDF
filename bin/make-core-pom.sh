#!/bin/bash

cd `dirname $0` ; source core-env.sh ; cd .. ; pwd

echo_run mvn archetype:generate -DgroupId=$GROUPID -DartifactId=$ARTIFACTID -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false
