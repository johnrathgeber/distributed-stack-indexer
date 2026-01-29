#!/bin/bash
# This is a student test

T_FOLDER=${T_FOLDER:-t}
R_FOLDER=${R_FOLDER:-}

cd "$(dirname "$0")/../..$R_FOLDER" || exit 1

DIFF=${DIFF:-diff}

term="boat"

cat "$T_FOLDER"/d/query_in2.txt > d/global-index.txt


if $DIFF <(./query.js "$term") <(cat "$T_FOLDER"/d/query_out2.txt) >&2;
then
    echo "$0 success: search results are identical"
    exit 0
else
    echo "$0 failure: search results are not identical"
    exit 1
fi
