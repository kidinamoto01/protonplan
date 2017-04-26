#!/usr/bin/env bash

# Don't edit the following line
export ROOT=$(dirname $(cd $(dirname $0); pwd))

export JAVA_HOME=${JAVA_HOME}
export LD_LIBRARY_PATH=/usr/hdp/2.5.3.0-37/hadoop/lib/native/
export HADOOP_CONF=/etc/hadoop/conf
export HBASE_CONF=/etc/hbase/conf
export HBASE_PATH=/usr/hdp/2.5.3.0-37/hbase/lib/*
CONF_PATH="$HADOOP_CONF:$HBASE_CONF"
export CLASSPATH="$CONF_PATH:$HBASE_PATH:$CLASSPATH"

# HBase namespace
HBASE_NAMESPACE=$(grep 'spark.count.hbase.namespace' $ROOT/conf/andlinks-spark.conf | awk -F'=' '{print $2}')
export HBASE_NAMESPACE=${HBASE_NAMESPACE//[[:blank:]]/}

echo "ROOT=$ROOT"
echo "JAVA_HOME=$JAVA_HOME"
echo "LD_LIBRARY_PATH=$LD_LIBRARY_PATH"
echo "HADOOP_CONF=$HADOOP_CONF"
echo "HBASE_CONF=$HBASE_CONF"
echo "CLASSPATH=$CLASSPATH"
echo "HBASE_NAMESPACE=$HBASE_NAMESPACE"

