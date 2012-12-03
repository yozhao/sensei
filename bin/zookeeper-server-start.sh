#!/bin/bash
base_dir=$(dirname $0)/..

if [ $# -ne 1 ];
then
	echo "USAGE: $0 zookeeper.properties"
	exit 1
fi

for file in $base_dir/sensei-core/target/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

if [ -z "$ZK_OPTS" ]; then
  ZK_OPTS="-Dlog4j.configuration=file:$base_dir/resources/zk-log4j.properties"
fi

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi


$JAVA $ZK_OPTS -cp $CLASSPATH org.apache.zookeeper.server.quorum.QuorumPeerMain $@
