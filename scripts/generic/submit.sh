#!/bin/bash
# $1 - ???

JAR="$1"
MASTER="$2"
START_DATE="$3"
END_DATE="$4"
AGGREGATION_INTERVAL="$5"
DUMP_DIR="$6"
OUTPUT_DIR="$7"

spark-submit \
  --master "$MASTER" \
  --conf "spark.eventLog.enabled=true" \
  --conf "spark.eventLog.dir=file:/tmp/spark-events" \
  --class pl.epsilondeltalimit.algosedd.AlgoSEDD \
  "$JAR" "$START_DATE" "$END_DATE" "$AGGREGATION_INTERVAL" "$DUMP_DIR" "$OUTPUT_DIR"