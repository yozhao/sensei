#!/usr/bin/env bash

#usage="Usage: start-sensei-node.sh <id> <port> <partitions> <conf-dir>"

# if no args specified, show usage
#if [ $# -le 3 ]; then
#  echo $usage
#  exit 1
#fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

OS=`uname`
IP="" # store IP
case $OS in
   Linux) IP=`/sbin/ifconfig  | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1}' | head -n 1`;;
   FreeBSD|OpenBSD|Darwin) IP=`ifconfig | grep -E '^en[0-9]:' -A 4 | grep -E 'inet.[0-9]' | grep -v '127.0.0.1' | awk '{ print $2}' | head -n 1` ;;
   SunOS) IP=`ifconfig -a | grep inet | grep -v '127.0.0.1' | awk '{ print $2} ' | head -n 1` ;;
   *) IP="Unknown";;
esac


lib=$bin/../sensei-core/target/lib
dist=$bin/../sensei-core/target
resources=$bin/../resources
gatewayslib=$bin/../sensei-gateways/target/lib
gatewaysdist=$bin/../sensei-gateways/target
logs=$bin/../logs

if [[ ! -d $logs ]]; then
  echo "Log file does not exists, creating one..."
  mkdir $logs
fi

# Min, max, total JVM size (-Xms -Xmx)
JVM_SIZE="-Xms15g -Xmx15g"

# New Generation Sizes (-XX:NewSize -XX:MaxNewSize)
JVM_SIZE_NEW="-XX:NewSize=8192m -XX:MaxNewSize=8192m"

# Type of Garbage Collector to use
JVM_GC_TYPE="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC"

# Tuning options for the above garbage collector
JVM_GC_OPTS="-XX:CMSInitiatingOccupancyFraction=75 -XX:SurvivorRatio=30"

# JVM GC activity logging settings ($LOG_DIR set in the ctl script)  ## DME: PrintTenuringDistribution not set in master_conf.sh
JVM_GC_LOG="-XX:+PrintTenuringDistribution -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:$logs/gc.log"


#JAVA_DEBUG="-Xdebug -Xrunjdwp:transport=dt_socket,address=1044,server=y,suspend=n"
JAVA_OPTS="-server -d64"
JMX_OPTS="-Djava.rmi.server.hostname=$IP -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=18889 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

MAIN_CLASS="com.senseidb.search.node.SenseiServer"

CLASSPATH=$resources/:$lib/*:$dist/*:$1/ext/*:$gatewayslib/*:$gatewaysdist/*

PIDFILE=/tmp/sensei-search-node.pid
if [ -f $PIDFILE ]; then
  echo "File $PIDFILE exists shutdown may not be proper"
  echo "Please check PID" `cat $PIDFILE`
  echo "Make sure the node is shutdown and the file" $PIDFILE "is removed before stating the node"
 else

  java $JAVA_OPTS $JMX_OPTS $JVM_SIZE $JVM_SIZE_NEW $JVM_GC_TYPE $JVM_GC_OPTS $JVM_GC_LOG $JAVA_DEBUG -classpath $CLASSPATH  -Dlog.home=$logs $MAIN_CLASS $1 &

  echo $! > ${PIDFILE}
  echo "Sensei node started successfully! Logs are at $logs"
 fi

