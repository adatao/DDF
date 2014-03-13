#!/bin/bash

echo $1
echo $2

MYLIST=`find $1 -type f -name *.scala`

for a in $MYLIST; do
    echo $a
done;

