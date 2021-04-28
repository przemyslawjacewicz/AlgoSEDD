#!/bin/bash
# downloads 7z archives from storage folder, extracts then and uploads extracted content to source folder
# dependencies: 7z

# $1 - archive folder uri e.g. gs://stack-exchange-data-dump/2020-12-08/

set -x #debug mode - will print commands

SOURCE_FOLDER_URI=${1%/}

# downloads a 7z archive from storage, extracts it and uploads extracted content to source folder
download_extract_upload_single() {
  # $1 - archive file uri e.g. gs://stack-exchange-data-dump/2020-12-08/3dprinting.meta.stackexchange.com.7z

  SOURCE_FILE_URI=${1%/}

  FILENAME=${SOURCE_FILE_URI##*/}
  COMMUNITY_NAME=${FILENAME%.*}

  DEST_FOLDER_URI=${SOURCE_FILE_URI%/*}/"$COMMUNITY_NAME"

  # test if already extracted
  gsutil -q stat "$DEST_FOLDER_URI"/* &>/dev/null
  DEST_FOLDER_EXISTS=$?

  if [ $DEST_FOLDER_EXISTS -eq 0 ]; then
    echo "File already processed: $SOURCE_SOURCE_FILE_URI"
    echo "Exiting."
    exit 0
  fi

  # download
  gsutil cp $SOURCE_SOURCE_FILE_URI $WORKING_DIR

  # extract
  7z e $WORKING_DIR/$FILENAME -o $WORKING_DIR/$COMMUNITY_NAME

  # upload
  gsutil -m cp -r $WORKING_DIR/$COMMUNITY_NAME/* $DEST_FOLDER_URI
}

WORKING_DIR=$(mktemp -d)

#todo: check if this glob is necessary
for f in $(gsutil ls $SOURCE_URI/*.???.7z); do
  download_extract_upload_single $f
done

# cleanup
rm -rf $WORKING_DIR
