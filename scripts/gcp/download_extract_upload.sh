#!/bin/bash
# $1 - ???

set -x #debug mode - will print commands

SOURCE_URI="$1"

for f in $(gsutil ls "$SOURCE_URI"/*.???.7z)
do
	bash download_extract_upload_archive.sh "$f"	
done
