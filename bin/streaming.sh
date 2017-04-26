#!/usr/bin/env bash


run () {
  if [ -f $RUN_PATH/$PID_FILE ]; then
    echo "$RUN_PATH/$PID_FILE already exists."
    echo "Now exiting ..."
    exit 1
  fi
  $@ > $LOG_PATH/$LOG_FILE 2>&1 &
  PID=$!
  echo $PID > "$RUN_PATH/$PID_FILE"
  wait $PID
  rm -f $RUN_PATH/$PID_FILE
}


BIN_DIR=$(cd $(dirname $0); pwd)
. $BIN_DIR/env.sh

LOG_PATH=$ROOT/logs
RUN_PATH=$ROOT/run

SPARK_SUBMIT='/usr/hdp/2.5.3.0-37/spark2/bin/spark-submit'
if [ "$JAVA_HOME" != "" ] ; then
  JAVA=$JAVA_HOME/bin/java
else
  echo "Environment variable \$JAVA_HOME is not set."
  exit 1
fi

if [ ! -d $LOG_PATH ];then
  mkdir -p $LOG_PATH
fi

if [ ! -d $RUN_PATH ];then
  mkdir -p $RUN_PATH
fi

CLASS="sheshou.writetosql.Write2Mysql"
LOG_FILE="streaming.out"
PID_FILE="streaming.pid"

CMD="$SPARK_SUBMIT --driver-class-path $CLASSPATH \
  --driver-cores 2 \
  --driver-memory 1G \
  --executor-cores 2 \
  --executor-memory 2G \
  --num-executors 5 \
  --properties-file $ROOT/conf/andlinks-spark.conf \
  --class $CLASS --master ${MASTER:-yarn-cluster} \
  $ROOT/lib/protonplan-1.0-SNAPSHOT.jar 10.20.10.2 /sheshou/data/parquet/attack_geo_distribution 10.20.10.1 sheshou attack_geo_distribution"
echo -e "$CMD"
run "$CMD" &

