#!/bin/bash

COUNT=0
MAX_COUNT=50

START_TIME=$(date +%s.%N)

while read -r url; do

  if [[ "$url" == "stop" ]] || [[ $COUNT -ge $MAX_COUNT ]]; then
    break;
  fi

  echo "[engine] crawling $url">/dev/stderr
  ./crawl.sh "$url" >d/content.txt

  COUNT=$((COUNT + 1))

  if  [[ "$(cat d/visited.txt | wc -l)" -ge "$(cat d/urls.txt | wc -l)" ]]; then
      break;
  fi

done < d/urls.txt

END_TIME=$(date +%s.%N)
ELAPSED=$(echo "$END_TIME - $START_TIME" | bc)
THROUGHPUT=$(echo "scale=2; $COUNT / $ELAPSED" | bc)

echo "Pages Crawled: $COUNT"
echo "Time Elapsed: $ELAPSED"
echo "Throughput: $THROUGHPUT"
