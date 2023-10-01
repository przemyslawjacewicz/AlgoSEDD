#!/bin/bash
# downloads 7z archives from storage folder, extracts them and uploads extracted content to source folder
# dependencies: 7z

# $1 - archive folder uri containing 7z archives e.g. gs://stack-exchange-data-dump/2020-12-08/

set -x #debug mode - will print commands

SOURCE_FOLDER_URI=${1%/}

# downloads a 7z archive from storage folder, extracts it and uploads extracted content to source folder
download_extract_upload_archive() {
  # $1 - archive file uri e.g. gs://stack-exchange-data-dump/2020-12-08/3dprinting.meta.stackexchange.com.7z

  ARCHIVE_FILE_URI=${1%/}

  FILENAME=${ARCHIVE_FILE_URI##*/}
  DEST_FOLDER_URI=${ARCHIVE_FILE_URI%.*}

  # test if already extracted
  gsutil -q stat "$DEST_FOLDER_URI"/* &>/dev/null
  DEST_FOLDER_EXISTS=$?

  if [ $DEST_FOLDER_EXISTS -eq 0 ]; then
    echo "File already processed: $ARCHIVE_FILE_URI"
    echo "Exiting."
    exit 0
  fi

  WORKING_DIR=$(mktemp -d)

  # download
  gsutil cp "$ARCHIVE_FILE_URI" "$WORKING_DIR"

  # extract
  7z e "$WORKING_DIR"/"$FILENAME" -o "$WORKING_DIR"

  # upload
  gsutil -m cp -r "$WORKING_DIR"/* "$DEST_FOLDER_URI"

  # cleanup
  rm -rf "$WORKING_DIR"
}

#todo: check if this glob is necessary
for f in $(gsutil ls "$SOURCE_FOLDER_URI"/*.???.7z); do
  download_extract_upload_single $f
done
