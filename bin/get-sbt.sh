#!/bin/bash

cd `dirname $0` ; pwd

[ -d sbt.dir ] && exit 0

VERSION=0.13.1
URL=http://repo.scala-sbt.org/scalasbt/sbt-native-packages/org/scala-sbt/sbt/$VERSION/sbt.zip
if hash wget 2>/dev/null; then
 wget -O /tmp/sbt.zip $URL
elif hash curl 2>/dev/null; then
 curl -o /tmp/sbt.zip $URL
else
 echo "You need curl or wget installed to download sbt."
 exit
fi

rm -fr sbt ; unzip /tmp/sbt.zip ; mv sbt sbt.dir
ln -s sbt.dir/bin/sbt sbt
