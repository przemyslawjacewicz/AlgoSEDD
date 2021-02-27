#!/bin/bash
#creates relative popularity plot for top N tags in a result bucket
#dependencies: gnuplot

set -x                     #debug mode - will print commands

RESULT_BUCKET_URI="${1%/}" #result bucket uri e.g. gs://plot-creation-testing_001/stackoverflow.com/8weeks
N="$2"                     #number of top tags to plot
START_DATE="$3"            #start data e.g. 2010-01-01
END_DATE="$4"              #end date e.g. 2021-01-01
YMAX="$5"                  #y axis max value

#TODO: add description
urlencode() {
  #urlencode <string>

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
  #urldecode <string>

  local url_encoded="${1//+/ }"
  printf '%b' "${url_encoded//%/\\x}"
}

YTICS=$(bc -l <<<$YMAX/10)

WORKING_DIR=$(mktemp -d)

AGGREGATION_INTERVAL=${RESULT_BUCKET_URI##*/}

tmp=${RESULT_BUCKET_URI%/*}
COMMUNITY_NAME=${tmp##*/}

#download tag info file
gsutil cp "$RESULT_BUCKET_URI"/part* "$WORKING_DIR"

TAGS_RESULT_FILE=$(ls "$WORKING_DIR"/part*)

n=1
declare -a TAGS
for f in $(head -"$(("$N" + 1))" "$TAGS_RESULT_FILE" | tail -"$N"); do
  #get tag name
  tag=$(cut -d',' -f2 <<<"$f")
  tag_urlencoded=$(urlencode "$tag")

  #download tag results
  gsutil cp -r "$RESULT_BUCKET_URI"/tag="$tag_urlencoded" "$WORKING_DIR"

  TAGS+=("$tag")

  n=$((n + 1))
done

#create strings for gnuplot 'word' function
FILES_STR=""
TAGS_STR=""
for tag in ${TAGS[@]}; do
  tag_urlencoded=$(urlencode "$tag")
  tag_result_file=$(ls "$WORKING_DIR"/tag="$tag_urlencoded"/part*)

  FILES_STR="$FILES_STR $tag_result_file"
  TAGS_STR="$TAGS_STR $tag"
done

relative_popularity_plot() {
  col="$1"
  plot_subtitle="$2"
  output_suffix="$3"

  gnuplot -persist <<-EOF
set datafile separator ','

set xdata time
set timefmt '%Y-%m-%d'
set xtics timedate
set xtics format '%Y-%m-%d'
set xtics rotate
set xtics '$START_DATE',31622400
set xrange ['$START_DATE':'$END_DATE']
set xlabel 'Date'

set yrange [0:$YMAX]
set ytics $YTICS
set ylabel 'Relative Popularity'

set style line 100 lt 1 lc rgb 'grey' lw 0.5
set grid ls 100

set terminal pngcairo size 800,600 enhanced font 'Segoe UI,10'

set title "community=$COMMUNITY_NAME    aggregation interval=$AGGREGATION_INTERVAL\n$plot_subtitle"

set output '${WORKING_DIR}/relative_popularity_single_${output_suffix}.png'

file_name(n)=word('$FILES_STR', n)
tag_name(n)=word('$TAGS_STR', n)
plot for [i=1:$N] file_name(i) using 1:$col with lines lw 2 title tag_name(i)
EOF
}

relative_popularity_plot 20 'questions + answers + comments + votes + post history + post links' 'qacvphpl'
relative_popularity_plot 19 'questions + answers + comments + votes + post history' 'qacvph'
relative_popularity_plot 18 'questions + answers + comments + votes' 'qacv'
relative_popularity_plot 17 'questions + answers + comments' 'qac'
relative_popularity_plot 16 'questions + answers' 'qa'
relative_popularity_plot 15 'questions' 'q'

#syncing
gsutil -m rsync -r "$WORKING_DIR" "$RESULT_BUCKET_URI"
