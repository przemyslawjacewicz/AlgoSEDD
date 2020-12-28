#!/bin/bash
# $1 - master
# $2 - application to run
# $3+ - application arguments

ARGS=("$@")
MASTER="$1"
APP="$2"
APP_ARGS=("${ARGS[@]:2}")

spark-submit \
  --master "$MASTER"\
  --jars "target/StackExchangeDataDumpAnalyzerSingle-0.1-SNAPSHOT.jar"\
  --packages "com.databricks:spark-xml_2.12:0.7.0","com.google.guava:guava:15.0"\
  --conf "spark.eventLog.enabled=true"\
  --conf "spark.eventLog.dir=file:/tmp/spark-events"\
  --class "$APP" target/StackExchangeDataDumpAnalyzerSingle-0.1-SNAPSHOT.jar "${APP_ARGS[@]}"