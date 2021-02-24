#!/bin/bash
#TODO: add description
#dependencies: git, java, maven

set -x                    #debug mode - will print commands

CLUSTER="$1"              #e.g. cluster-202d
REGION="$2"               #e.g. europe-west3
DUMP_BUCKET_URI="$3"      #e.g. gs://stack-exchange-data-dump/2020-12-08
COMMUNITY_NAME="$4"       #e.g. 3dprinting.stackexchange.com
AGGREGATION_INTERVAL="$5" #must be a valid Spark interval e.g. '12 weeks'
START_DATE="$6"           #e.g. 2010-01-01
END_DATE="$7"             #e.g. 2021-01-01
WORKING_BUCKET_URI="$8"   #optional e.g. gs://1a2b3c4d

# if WORKING_BUCKET_URI is empty create the bucket
if [ -z "$WORKING_BUCKET_URI" ]; then
  working_bucket_name=$(cat /dev/urandom | tr -dc 'a-z0-9' | fold -w 8 | head -n 1) #TODO: consider different naming pattern
  WORKING_BUCKET_URI=gs://"$working_bucket_name"
  gsutil mb -l "$REGION" "$WORKING_BUCKET_URI"
fi

# if no app jar in result bucket build and uplod it
APP_JAR_URI=$(gsutil ls "$WORKING_BUCKET_URI"/*.jar)
if [ -z "$APP_JAR_URI" ]; then
  # create temp working dir
  working_dir=$(mktemp -d)

  # checkout repo
  git clone https://github.com/przemyslawjacewicz/StackExchangeDataDumpAnalyzerSingle "$working_dir" #TODO: must supply credentials or be a public repo

  # build jar
  (cd "$working_dir" && mvn clean package)
  app_jar=$(ls "$working_dir"/target/*-with-dependencies.jar)
  app_jar_name=${app_jar##*/}

  # upload to a working bucket
  gsutil cp "$app_jar" "$WORKING_BUCKET_URI"
  APP_JAR_URI="$WORKING_BUCKET_URI"/"$app_jar_name"
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
  --class=pl.epsilondeltalimit.analyzer.StackExchangeDataDumpAnalyzerSingle \
  -- "$START_DATE" "$END_DATE" "$AGGREGATION_INTERVAL" "$BADGES_FILE_URI" "$COMMENTS_FILE_URI" "$POSTHISTORY_FILE_URI" "$POSTLINKS_FILE_URI" "$POSTS_FILE_URI" "$TAGS_FILE_URI" "$USERS_FILE_URI" "$VOTES_FILE_URI" "$OUTPUT_DIR_URI"
