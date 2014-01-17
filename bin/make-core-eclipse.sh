#!/bin/bash

cd `dirname $0` ; source core-env.sh ; cd ../$ARTIFACTID ; pwd

echo_run mvn eclipse:eclipse
