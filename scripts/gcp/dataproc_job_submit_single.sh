#!/bin/bash
# $1 - ???

set -x #debug mode - will print commands

REPO="https://github.com/przemyslawjacewicz/StackExchangeDataDumpAnalyzerSingle"

ARGS=("$@")
CLUSTER="$1"
REGION="$2"
APP_JAR_URI="$3" # can be a valid app jar uri
APP="$4"
APP_ARGS=("${ARGS[@]:4}")

#check if already done


# check if APP_JAR_URI exists
gsutil -q stat "$APP_JAR_URI"/* &> /dev/null
APP_JAR_URI_EXISTS=$?

# if app jar is not uploaded then we build jar with dependencies and upload it to a bucket
if [ APP_JAR_URI_EXISTS -ne 0 ]
then
  # build jar
  WORKING_DIR=$(mktemp -d)
  git clone "$REPO" "$WORKING_DIR" # must supply credentials or be a public repo
  cd "$WORKING_DIR" && mvn clean package
  APP_JAR_FILENAME=$(cd "$WORKING_DIR"/target && ls *-with-dependencies.jar)
  LOCAL_APP_JAR="$WORKING_DIR"/target/"$APP_JAR_FILENAME"

  # create bucket
  APP_JAR_BUCKET_NAME=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1) #probably only lower case letters are possible
  gsutil mb gs://"$APP_JAR_BUCKET_NAME"

  # upload to a new bucket
  gsutil cp "$LOCAL_APP_JAR" gs://"$APP_JAR_BUCKET_NAME"

  APP_JAR_URI=gs://"$APP_JAR_BUCKET_NAME"/
fi

# submit job
gcloud dataproc jobs submit spark \
  --cluster="$CLUSTER" \
  --region="$REGION" \
  --jars="$APP_JAR_URI" \
  --class="$APP" \
  -- "${APP_ARGS[@]}"