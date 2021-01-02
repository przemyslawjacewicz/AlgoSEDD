#!/bin/bash

FILE=$1
TAG=$2
AGGREGATION_INTERVAL=$3
YMAX=$4
YTICS=$5

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

set title '${TAG}     aggregation interval=${AGGREGATION_INTERVAL}'

set terminal pngcairo size 800,600 enhanced font 'Segoe UI,10'
set output 'relative_popularity__${TAG}_${AGGREGATION_INTERVAL}.png'

plot '$FILE' using 1:20 with lines ls 106 title 'questions + answers + comments + votes + post history + post links',\
  '' using 1:19 with lines ls 105 title 'questions + answers + comments + votes + post history',\
  '' using 1:18 with lines ls 104 title 'questions + answers + comments + votes',\
  '' using 1:17 with lines ls 103 title 'questions + answers + comments',\
  '' using 1:16 with lines ls 102 title 'questions + answers',\
  '' using 1:15 with lines ls 101 title 'questions'
EOF
