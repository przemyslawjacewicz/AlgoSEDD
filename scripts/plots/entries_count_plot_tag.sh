#!/bin/bash
# $1 - csv result file
# $2 - tag name
# $3 - aggregation interval, spaces removed
# $4 - y axis max value
# $5 - optional output dir, if missing will save in current dir

FILE="$1"
TAG="$2"
AGGREGATION_INTERVAL="$3"
YMAX="$4"
DIR="$5"

# if DIR is empty write result to current dir
if [ -z $DIR ]
then
  DIR=$PWD
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

set title '${TAG}   aggregation interval=${AGGREGATION_INTERVAL}'

set terminal pngcairo size 800,600 enhanced font 'Segoe UI,10'
set output '$DIR/entries_count__${TAG}_${AGGREGATION_INTERVAL}.png'

plot '${FILE}' using 1:8 with boxes ls 106 title 'questions + answers + comments + votes + post history + post links',\
'' using 1:7 with boxes ls 105 title 'questions + answers + comments + votes + post history',\
'' using 1:6 with boxes ls 104 title 'questions + answers + comments + votes',\
'' using 1:5 with boxes ls 103 title 'questions + answers + comments',\
'' using 1:4 with boxes ls 102 title 'questions + answers',\
'' using 1:3 with boxes ls 101 title 'questions'
EOF
