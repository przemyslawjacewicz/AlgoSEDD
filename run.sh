#!/bin/bash
# $1 - application to run
# $2 - master
# $3+ - application arguments

ARGS=("$@")
APP="$1"
MASTER="$2"
APP_ARGS=("${ARGS[@]:2}")

spark-submit \
  --master "$MASTER" \
  --packages "com.databricks:spark-xml_2.12:0.7.0","com.google.guava:guava:15.0" \
  --conf "spark.eventLog.enabled=true" \
  --conf "spark.eventLog.dir=file:/tmp/spark-events" \
  --class "$APP" target/StackExchangeDataDumpAnalyzerSingle-0.1-SNAPSHOT.jar "${APP_ARGS[@]}"