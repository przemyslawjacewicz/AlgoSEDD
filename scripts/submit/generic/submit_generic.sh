#!/bin/bash
#todo: consider removing
# $1 - application to run
# $2 - master
# $3+ - application arguments

ARGS=("$@")
JAR="$1"
APP="$2"
MASTER="$3"
APP_ARGS=("${ARGS[@]:3}")

spark-submit \
  --master "$MASTER" \
  --packages "com.databricks:spark-xml_2.12:0.7.0","com.google.guava:guava:15.0" \
  --conf "spark.eventLog.enabled=true" \
  --conf "spark.eventLog.dir=file:/tmp/spark-events" \
  --class "$APP" \
  "$JAR" "${APP_ARGS[@]}"