#!/bin/bash

cd `dirname $0` ; pwd

[ -d sbt.dir ] && exit 0

VERSION=0.13.1
URL=http://repo.scala-sbt.org/scalasbt/sbt-native-packages/org/scala-sbt/sbt/$VERSION/sbt.zip
wget -O /tmp/sbt.zip $URL
rm -fr sbt ; unzip /tmp/sbt.zip ; mv sbt sbt.dir
ln -s sbt.dir/bin/sbt sbt
