#!/bin/bash
#TODO: add dependecies
#TODO: add position number formatting 000,001,...
# creates entries count plots for top N tags in a result bucket

# $1 - result bucket uri e.g. gs://plot-creation-testing_001/stackoverflow.com/8weeks
# $2 - number of top tags to plot

set -x #debug mode - will print commands

RESULT_BUCKET_URI="${1%/}"
N="$2"

#TODO: add description
urlencode() {
  # urlencode <string>

  local length="${#1}"
  for ((i = 0; i < length; i++)); do
    local c="${1:i:1}"
    case $c in
    [a-zA-Z0-9.~_-]) printf "$c" ;;
    *) printf '%s' "$c" | xxd -p -c1 |
      while read c; do printf '%%%s' "$c"; done ;;
    esac
  done
}

#TODO: add description
urldecode() {
  # urldecode <string>

  local url_encoded="${1//+/ }"
  printf '%b' "${url_encoded//%/\\x}"
}

# creates relative popularity plot from single tag results
entries_count_plot_tag() {
  # $1 - csv result file
  # $2 - community name
  # $3 - aggregation interval, spaces removed
  # $4 - tag name
  # $5 - tag popularity position
  # $6 - y axis max value
  # $7 - optional output dir, if missing will save in current dir

  FILE="$1"
  COMMUNITY_NAME="$2"
  AGGREGATION_INTERVAL="$3"
  TAG=$(urldecode "$4")
  POPULARITY_POS="$5"
  YMAX="$6"
  OUTPUT_DIR="$7"

  # if OUTPUT_DIR is empty write result to current dir
  if [ -z "$OUTPUT_DIR" ]; then
    OUTPUT_DIR=$PWD
  fi

  # create plot
  gnuplot -persist <<-EOF
set datafile separator ','

set xdata time
set timefmt '%Y-%m-%d'
set xtics timedate
set xtics format '%Y-%m-%d'
set xtics rotate
set xtics '2010-01-01',31622400
set xrange ['2010-01-01':'2021-01-01']
set xlabel 'Date'

set logscale y
set ytics 1,10
set yrange [1:$YMAX]
set ylabel 'Entries count'

set key outside bottom center Left reverse

set style line 100 lt 1 lc rgb 'grey' lw 0.5
set grid ls 100

set style line 101 lw 1 lt rgb '#2d728f'
set style line 102 lw 1 lt rgb '#3b8ea5'
set style line 103 lw 1 lt rgb '#f5ee9e'
set style line 104 lw 1 lt rgb '#f49e4c'
set style line 105 lw 1 lt rgb '#ab3428'
set style line 106 lw 1 lt rgb '#73201b'
set style fill solid noborder

set title 'community=${COMMUNITY_NAME}    aggregation interval=${AGGREGATION_INTERVAL}    tag=${TAG}'

set terminal pngcairo size 800,600 enhanced font 'Segoe UI,10'
set output '${OUTPUT_DIR}/${POPULARITY_POS}__entries_count.png'

plot '${FILE}' using 1:8 with boxes ls 106 title 'questions + answers + comments + votes + post history + post links',\
'' using 1:7 with boxes ls 105 title 'questions + answers + comments + votes + post history',\
'' using 1:6 with boxes ls 104 title 'questions + answers + comments + votes',\
'' using 1:5 with boxes ls 103 title 'questions + answers + comments',\
'' using 1:4 with boxes ls 102 title 'questions + answers',\
'' using 1:3 with boxes ls 101 title 'questions'
EOF
}

WORKING_DIR=$(mktemp -d)

AGGREGATION_INTERVAL=${RESULT_BUCKET_URI##*/}

tmp=${RESULT_BUCKET_URI%/*}
COMMUNITY_NAME=${tmp##*/}

# download tag info file
gsutil cp "$RESULT_BUCKET_URI"/part* "$WORKING_DIR"

TAGS_RESULT_FILE=$(ls "$WORKING_DIR"/part*)

n=1
for f in $(head -$(("$N" + 1)) "$TAGS_RESULT_FILE" | tail -"$N"); do
  # get tag
  tag=$(cut -d',' -f2 <<<"$f")
  tag_urlencoded=$(urlencode "$tag")

  # download tag resutls
  gsutil cp -r "$RESULT_BUCKET_URI"/tag="$tag_urlencoded" "$WORKING_DIR"

  # create plot
  tag_result_file=$(ls "$WORKING_DIR"/tag="$tag_urlencoded"/part*)
  entries_count_plot_tag "$tag_result_file" "$COMMUNITY_NAME" "$AGGREGATION_INTERVAL" "$tag" "$n" 100000 "$WORKING_DIR" #TODO: move 100000 to script params
  n=$((n + 1))
done

# syncing
gsutil -m rsync -r "$WORKING_DIR" "$RESULT_BUCKET_URI"
