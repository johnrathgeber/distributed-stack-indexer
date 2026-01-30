#!/bin/bash

T_FOLDER=${T_FOLDER:-t}
R_FOLDER=${R_FOLDER:-}

term="stuff" # note that this tests the same exact query 50 times (same exact term and global index)

COUNT=50

cat "$T_FOLDER"/d/d7.txt > d/global-index.txt

START_TIME=$(date +%s.%N)

for _ in $(seq 1 $COUNT); do
  ./query.js "$term" >/dev/null
done

END_TIME=$(date +%s.%N)
ELAPSED=$(echo "$END_TIME - $START_TIME" | bc)
THROUGHPUT=$(echo "scale=4; $COUNT / $ELAPSED" | bc)

echo "Queries: $COUNT"
echo "Time Elapsed: $ELAPSED"
echo "Throughput: $THROUGHPUT"
