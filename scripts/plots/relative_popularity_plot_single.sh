#!/bin/bash
# creates relative popularity plots for all tags in a result bucket

# $1 - result bucket uri that is if results are in gs://plot-creation-testing_001/stackoverflow.com/8weeks this should be gs://plot-creation-testing_001

set -x #debug mode - will print commands

RESULT_BUCKET_URI="${1%/}"

# creates relative popularity plot from single tag results
relative_popularity_plot_tag() {
  # $1 - csv result file
  # $2 - community name
  # $3 - aggregation interval, spaces removed
  # $4 - tag name
  # $5 - y axis max value
  # $6 - y axis tics value
  # $7 - optional output dir, if missing will save in current dir

  FILE="$1"
  COMMUNITY_NAME="$2"
  AGGREGATION_INTERVAL="$3"
  TAG="$4"
  YMAX="$5"
  YTICS="$6"
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

set yrange [0:$YMAX]
set ytics $YTICS
set ylabel 'Relative popularity'

set key outside bottom center Left reverse

set style line 100 lt 1 lc rgb 'grey' lw 0.5
set grid ls 100

set style line 101 lw 2 lt rgb '#2d728f'
set style line 102 lw 2 lt rgb '#3b8ea5'
set style line 103 lw 2 lt rgb '#f5ee9e'
set style line 104 lw 2 lt rgb '#f49e4c'
set style line 105 lw 2 lt rgb '#ab3428'
set style line 106 lw 2 lt rgb '#73201b'

set title 'community=${COMMUNITY_NAME}    aggregation interval=${AGGREGATION_INTERVAL}    ${TAG}'

set terminal pngcairo size 800,600 enhanced font 'Segoe UI,10'
set output '$OUTPUT_DIR/relative_popularity.png'

plot '$FILE' using 1:20 with lines ls 106 title 'questions + answers + comments + votes + post history + post links',\
  '' using 1:19 with lines ls 105 title 'questions + answers + comments + votes + post history',\
  '' using 1:18 with lines ls 104 title 'questions + answers + comments + votes',\
  '' using 1:17 with lines ls 103 title 'questions + answers + comments',\
  '' using 1:16 with lines ls 102 title 'questions + answers',\
  '' using 1:15 with lines ls 101 title 'questions'
EOF
}

WORKING_DIR=$(mktemp -d)

# download result bucket
gsutil -m cp -r "$RESULT_BUCKET_URI"/* "$WORKING_DIR"

COMMUNITY_NAME=$(ls "$WORKING_DIR")
AGGREGATION_INTERVAL=$(ls "$WORKING_DIR"/"$COMMUNITY_NAME")

for f in "$WORKING_DIR"/"$COMMUNITY_NAME"/"$AGGREGATION_INTERVAL"/*; do
  TAG_STR=${f##*/}
  relative_popularity_plot_tag "$(ls "$f"/part*)" "$COMMUNITY_NAME" "$AGGREGATION_INTERVAL" "$TAG_STR" 1.0 0.1 "$f" #TODO: move 1.0 and 0.1 to script params
done

# syncing
gsutil -m rsync -r "$WORKING_DIR" "$RESULT_BUCKET_URI"
