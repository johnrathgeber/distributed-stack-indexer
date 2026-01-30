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
  echo "[engine] indexing $url">/dev/stderr
  ./index.sh d/content.txt "$url"

  COUNT=$((COUNT + 1))

  if  [[ "$(cat d/visited.txt | wc -l)" -ge "$(cat d/urls.txt | wc -l)" ]]; then
      break;
  fi

done < d/urls.txt

END_TIME=$(date +%s.%N)
ELAPSED=$(echo "$END_TIME - $START_TIME" | bc) # NOTE: TEST CRAWL DURATION FIRST, THEN REPLACE 42
ELAPSED_WITHOUT=$(echo "$END_TIME - $START_TIME - 42" | bc) # 42 is time to crawl 50 urls -- value to be replaced
THROUGHPUT=$(echo "scale=2; $COUNT / $ELAPSED_WITHOUT" | bc)

echo "Pages Crawled/Indexed: $COUNT"
echo "Total Time Elapsed: $ELAPSED"
echo "Time Elapsed Indexing: $ELAPSED_WITHOUT"
echo "Throughput: $THROUGHPUT"
