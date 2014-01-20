#!/bin/bash

VERSION=0.13.1
URL=http://repo.scala-sbt.org/scalasbt/sbt-native-packages/org/scala-sbt/sbt/$VERSION/sbt.zip

function usage {
	echo ""
	echo "    Usage: $0 [-f]"
	echo ""
	echo "    Attempts to retrieve SBT $VERSION from $URL."
	echo "    If sbt.dir already exists, this will be skipped, unless the -f flag is specified."
	echo ""
	exit 1
}

cd `dirname $0` ; pwd

[ "$1" != "-f" -a -d sbt.dir ] && usage

if which curl ; then
	curl  -o /tmp/sbt.zip $URL
elif which wget ; then
	wget -O /tmp/sbt.zip $URL
else
	echo "ERROR: must have either curl or wget to retrieve sbt from $URL"
	exit 1
fi

rm -fr sbt sbt.dir ; unzip /tmp/sbt.zip ; mv sbt sbt.dir
ln -s sbt.dir/bin/sbt sbt
