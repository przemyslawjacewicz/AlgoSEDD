#!/bin/bash
#submits StackExchangeDataDumpAnalyzerSingle Spark job to dataproc cluster
#dependencies: git, java, maven
#TODO: add proper handling of versioning
#TODO: change the name of app to be launched because it is the same as project name

set -x                    #debug mode - will print commands

CLUSTER="$1"              #cluster name e.g. cluster-202d
REGION="$2"               #region e.g. europe-west3
DUMP_BUCKET_URI="$3"      #StackExchange dump bucket uri e.g. gs://stack-exchange-data-dump/2020-12-08
COMMUNITY_NAME="$4"       #communit name e.g. 3dprinting.stackexchange.com
AGGREGATION_INTERVAL="$5" #aggregation interval, must be a valid Spark interval e.g. '12 weeks'
START_DATE="$6"           #start data e.g. 2010-01-01
END_DATE="$7"             #end date e.g. 2021-01-01
RESULT_BUCKET_URI="$8"    #result bucket uri, optional e.g. gs://1a2b3c4d

#if RESULT_BUCKET_URI variable is empty create the bucket
if [ -z "$RESULT_BUCKET_URI" ]; then
  working_bucket_name=$(cat /dev/urandom | tr -dc 'a-z0-9' | fold -w 8 | head -n 1) #TODO: consider different naming pattern
  RESULT_BUCKET_URI=gs://"$working_bucket_name"
  gsutil mb -l "$REGION" "$RESULT_BUCKET_URI"
fi

#if no app jar in result bucket build and upload it
APP_JAR_URI=$(gsutil ls "$RESULT_BUCKET_URI"/*.jar)
if [ -z "$APP_JAR_URI" ]; then
  #create temp working dir
  working_dir=$(mktemp -d)

  #checkout repo
  git clone https://github.com/przemyslawjacewicz/StackExchangeDataDumpAnalyzerSingle "$working_dir" #TODO: must supply credentials or be a public repo

  #build jar
  (cd "$working_dir" && mvn clean package)
  app_jar=$(ls "$working_dir"/target/*-with-dependencies.jar)
  app_jar_name=${app_jar##*/}

  #build version
  (cd "$working_dir" $$ git log --pretty=oneline -1 >app.version)

  #upload to a result bucket
  gsutil cp "$app_jar" "$RESULT_BUCKET_URI"
  APP_JAR_URI="$RESULT_BUCKET_URI"/"$app_jar_name"
fi

#submit job
BADGES_FILE_URI="$DUMP_BUCKET_URI"/"$COMMUNITY_NAME"/Badges.xml
COMMENTS_FILE_URI="$DUMP_BUCKET_URI"/"$COMMUNITY_NAME"/Comments.xml
POSTHISTORY_FILE_URI="$DUMP_BUCKET_URI"/"$COMMUNITY_NAME"/PostHistory.xml
POSTLINKS_FILE_URI="$DUMP_BUCKET_URI"/"$COMMUNITY_NAME"/PostLinks.xml
POSTS_FILE_URI="$DUMP_BUCKET_URI"/"$COMMUNITY_NAME"/Posts.xml
TAGS_FILE_URI="$DUMP_BUCKET_URI"/"$COMMUNITY_NAME"/Tags.xml
USERS_FILE_URI="$DUMP_BUCKET_URI"/"$COMMUNITY_NAME"/Users.xml
VOTES_FILE_URI="$DUMP_BUCKET_URI"/"$COMMUNITY_NAME"/Votes.xml
OUTPUT_DIR_URI="$RESULT_BUCKET_URI"/"$COMMUNITY_NAME"/"${AGGREGATION_INTERVAL// /}"
gcloud dataproc jobs submit spark \
  --cluster="$CLUSTER" \
  --region="$REGION" \
  --jars="$APP_JAR_URI" \
  --class=pl.epsilondeltalimit.analyzer.StackExchangeDataDumpAnalyzerSingle \
  -- "$START_DATE" "$END_DATE" "$AGGREGATION_INTERVAL" "$BADGES_FILE_URI" "$COMMENTS_FILE_URI" "$POSTHISTORY_FILE_URI" "$POSTLINKS_FILE_URI" "$POSTS_FILE_URI" "$TAGS_FILE_URI" "$USERS_FILE_URI" "$VOTES_FILE_URI" "$OUTPUT_DIR_URI"
