#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

home=`cd "$bin/..";pwd`

java -classpath target/lib/*:target/* com.senseidb.tools.IndexSplitter $1 $2 $3 $4
