#!/bin/bash
#TODO: add description
#TODO: change the flow of this script - checkout git repo, create bucket with upload jar, run job with output set to new bucket
#TODO: dependencies: git, java, maven

# $1 - ???

set -x    #debug mode - will print commands

REPO="$1" #"https://github.com/przemyslawjacewicz/StackExchangeDataDumpAnalyzerSingle"
CLUSTER="$2"
REGION="$3"
APP="$4"
START_DATE="$5"
END_DATE="$6"
AGGREGATION_INTERVAL="$7"
DUMP_BUCKET_URI="$8"
COMMUNITY_NAME="$9"
WORKING_BUCKET_URI="${10}" #optional

# if WORKING_BUCKET_URI is empty create the bucket
if [ -z "$WORKING_BUCKET_URI" ]; then
  WORKING_BUCKET_NAME=$(cat /dev/urandom | tr -dc 'a-z0-9' | fold -w 8 | head -n 1) #TODO: consider different naming pattern
  WORKING_BUCKET_URI=gs://"$WORKING_BUCKET_NAME"
  gsutil mb "$WORKING_BUCKET_URI" #todo: add region
fi

# if no app jar in result bucket build and uplod it
APP_JAR_URI=$(gsutil ls "$WORKING_BUCKET_URI"/*.jar)
APP_JAR_EXISTS=$(expr match "$APP_JAR_URI" "$WORKING_BUCKET_URI")
if [ "$APP_JAR_EXISTS" -eq 0 ]; then
  # create temp working dir
  WORKING_DIR=$(mktemp -d)

  # checkout repo
  git clone "$REPO" "$WORKING_DIR" #TODO: must supply credentials or be a public repo

  # build jar
  (cd "$WORKING_DIR" && mvn clean package)
  APP_JAR=$(ls "$WORKING_DIR"/target/*-with-dependencies.jar)
  APP_JAR_NAME=${APP_JAR##*/}

  # upload to a working bucket
  gsutil cp "$APP_JAR" "$WORKING_BUCKET_URI"
  APP_JAR_URI="$WORKING_BUCKET_URI"/"$APP_JAR_NAME"
fi

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
