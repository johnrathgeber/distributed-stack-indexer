#!/bin/bash

COUNT=0
MAX_COUNT=50

URLS_SNAPSHOT=$(mktemp)
URLS_SNAPSHOT2=$(mktemp)
cp d/urls.txt "$URLS_SNAPSHOT"
cp d/urls.txt "$URLS_SNAPSHOT2"

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

  if  [[ "$(cat d/visited.txt | wc -l)" -ge "$(cat "$URLS_SNAPSHOT2" | wc -l)" ]]; then
      break;
  fi

done < "$URLS_SNAPSHOT"

rm -f "$URLS_SNAPSHOT"
rm -f "$URLS_SNAPSHOT2"

END_TIME=$(date +%s.%N)
ELAPSED=$(echo "$END_TIME - $START_TIME" | bc) # NOTE: TEST CRAWL DURATION FIRST, THEN REPLACE 83
ELAPSED_WITHOUT=$(echo "$END_TIME - $START_TIME - 83" | bc) # 83 is time to crawl 50 urls -- value to be replaced
THROUGHPUT=$(echo "scale=2; $COUNT / $ELAPSED_WITHOUT" | bc)

echo "Pages Crawled/Indexed: $COUNT"
echo "Total Time Elapsed: $ELAPSED"
echo "Time Elapsed Indexing: $ELAPSED_WITHOUT"
echo "Throughput: $THROUGHPUT"
