#!/bin/bash
# $1 - ???

set -x #debug mode - will print commands

SOURCE_URI="$1"

FILENAME=${SOURCE_URI##*/}
COMMUNITY_NAME=${FILENAME%.*}

DEST_URI=${SOURCE_URI%/*}/"$COMMUNITY_NAME"

# test if already extracted
gsutil -q stat "$DEST_URI"/* &> /dev/null
DEST_EXISTS=$?

if [ $DEST_EXISTS -eq 0 ]
then
	echo "File already processed: $SOURCE_URI"
	echo "Exiting."
	exit 0
fi

WORKING_DIR=$(mktemp -d)

# download
gsutil cp "$SOURCE_URI" "$WORKING_DIR"

# extract
7z e "$WORKING_DIR"/"$FILENAME" -o"$WORKING_DIR"/"$COMMUNITY_NAME"

# upload
gsutil -m cp -r "$WORKING_DIR"/"$COMMUNITY_NAME"/* "$DEST_URI"

# cleanup
rm -rf "$WORKING_DIR"
