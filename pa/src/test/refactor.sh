#!/bin/bash

echo $1
echo $2

MYLIST=`find $1 -type f -name $2`

for a in $MYLIST; do
    echo $a
    mv $a $a.orig
    sed s/$3/$4/g $a.orig > $a
    rm $a.orig
done;

