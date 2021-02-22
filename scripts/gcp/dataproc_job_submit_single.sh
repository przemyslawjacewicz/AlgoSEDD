#!/bin/bash
#TODO: add description
#TODO: change the flow of this script - checkout git repo, create bucket with upload jar, run job with output set to new bucket
#TODO: dependencies: git, java, maven

# $1 - ???

set -x #debug mode - will print commands

REPO="$1" #"https://github.com/przemyslawjacewicz/StackExchangeDataDumpAnalyzerSingle"
CLUSTER="$2"
REGION="$3"
APP="$4"
START_DATE="$5"
END_DATE="$6"
AGGREGATION_INTERVAL="$7"
DUMP_BUCKET_URI="$8"
COMMUNITY_NAME="$9"

WORKING_DIR=$(mktemp -d)

# checkout repo
git clone "$REPO" "$WORKING_DIR" #TODO: must supply credentials or be a public repo

# build jar
(cd "$WORKING_DIR" && mvn clean package)
APP_JAR=$(ls "$WORKING_DIR"/target/*-with-dependencies.jar)
APP_JAR_NAME=${APP_JAR##*/}

# create working bucket
WORKING_BUCKET_NAME=$(cat /dev/urandom | tr -dc 'a-z0-9' | fold -w 8 | head -n 1) #probably only lower case letters are possible
WORKING_BUCKET_URI=gs://"$WORKING_BUCKET_NAME"
gsutil mb "$WORKING_BUCKET_URI"

# upload to a working bucket
gsutil cp "$APP_JAR" "$WORKING_BUCKET_URI"
APP_JAR_URI="$WORKING_BUCKET_URI"/"$APP_JAR_NAME"

# submit job
BADGES_FILE_URI="$DUMP_BUCKET_URI"/"$COMMUNITY_NAME"/Badges.xml
COMMENTS_FILE_URI="$DUMP_BUCKET_URI"/"$COMMUNITY_NAME"/Comments.xml
POSTHISTORY_FILE_URI="$DUMP_BUCKET_URI"/"$COMMUNITY_NAME"/PostHistory.xml
POSTLINKS_FILE_URI="$DUMP_BUCKET_URI"/"$COMMUNITY_NAME"/PostLinks.xml
POSTS_FILE_URI="$DUMP_BUCKET_URI"/"$COMMUNITY_NAME"/Posts.xml
TAGS_FILE_URI="$DUMP_BUCKET_URI"/"$COMMUNITY_NAME"/Tags.xml
USERS_FILE_URI="$DUMP_BUCKET_URI"/"$COMMUNITY_NAME"/Users.xml
VOTES_FILE_URI="$DUMP_BUCKET_URI"/"$COMMUNITY_NAME"/Votes.xml
OUTPUT_DIR_URI="$WORKING_BUCKET_URI"/"$COMMUNITY_NAME"/"${AGGREGATION_INTERVAL// /}"
gcloud dataproc jobs submit spark \
  --cluster="$CLUSTER" \
  --region="$REGION" \
  --jars="$APP_JAR_URI" \
  --class="$APP" \
  -- "$START_DATE" "$END_DATE" "$AGGREGATION_INTERVAL" "$BADGES_FILE_URI" "$COMMENTS_FILE_URI" "$POSTHISTORY_FILE_URI" "$POSTLINKS_FILE_URI" "$POSTS_FILE_URI" "$TAGS_FILE_URI" "$USERS_FILE_URI" "$VOTES_FILE_URI" "$OUTPUT_DIR_URI"