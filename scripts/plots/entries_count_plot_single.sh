#!/bin/bash
# creates relative popularity plots for all tags in a result bucket
#TODO: consider moving the script for plot creation inside this script

# $1 - result buclet uri that is if results are in gs://plot-creation-testing_001/stackoverflow.com/8weeks/ this should be gs://plot-creation-testing_001

set -x #debug mode - will print commands

RESULT_BUCKET_URI="${1%/}"

WORKING_DIR=$(mktemp -d)

# download result bucket
gsutil -m cp -r "$RESULT_BUCKET_URI"/* "$WORKING_DIR"

COMMUNITY_NAME=$(ls "$WORKING_DIR")
AGGREGATION_INTERVAL=$(ls "$WORKING_DIR"/"$COMMUNITY_NAME")

for f in "$WORKING_DIR"/"$COMMUNITY_NAME"/"$AGGREGATION_INTERVAL"/*; do
  TAG_STR=${f##*/}
  bash entries_count_plot_tag.sh "$(ls "$f"/part*)" "$TAG_STR" "$AGGREGATION_INTERVAL" 100000 "$CURRENT_DIR" #TODO: move 100000 to script params
done

# syncing
gsutil -m rsync -r "$WORKING_DIR" "$RESULT_BUCKET_URI"
