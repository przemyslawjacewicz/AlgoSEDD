#!/bin/bash
# $1 - ???

JAR="$1"
MASTER="$2"
START_DATE="$3"
END_DATE="$4"
AGGREGATION_INTERVAL="$5"
DUMP_DIR="$6"
COMMUNITY="$7"
OUTPUT_DIR="$8"

BADGES_FILE=$DUMP_DIR/$COMMUNITY/Badges.xml
COMMENTS_FILE=$DUMP_DIR/$COMMUNITY/Comments.xml
POSTHISTORY_FILE=$DUMP_DIR/$COMMUNITY/PostHistory.xml
POSTLINKS_FILE=$DUMP_DIR/$COMMUNITY/PostLinks.xml
POSTS_FILE=$DUMP_DIR/$COMMUNITY/Posts.xml
TAGS_FILE=$DUMP_DIR/$COMMUNITY/Tags.xml
USERS_FILE=$DUMP_DIR/$COMMUNITY/Users.xml
VOTES_FILE=$DUMP_DIR/$COMMUNITY/Votes.xml
OUTPUT=$OUTPUT_DIR/$COMMUNITY/${AGGREGATION_INTERVAL// /}

spark-submit \
  --master "$MASTER" \
  --packages "com.databricks:spark-xml_2.12:0.7.0","com.google.guava:guava:15.0" \
  --conf "spark.eventLog.enabled=true" \
  --conf "spark.eventLog.dir=file:/tmp/spark-events" \
  --class pl.epsilondeltalimit.analyzer.StackExchangeDataDumpAnalyzerSingle \
  "$JAR" "$START_DATE" "$END_DATE" "$AGGREGATION_INTERVAL" "$BADGES_FILE" "$COMMENTS_FILE" "$POSTHISTORY_FILE" "$POSTLINKS_FILE" "$POSTS_FILE" "$TAGS_FILE" "$USERS_FILE" "$VOTES_FILE" "$OUTPUT"