# Distributed Search Engine — LKML

[![Project Poster](https://www.johnrathgeber.com/Distributed_Poster_png.png)](https://www.johnrathgeber.com/Distributed_Poster.pdf)

A distributed search engine built from scratch in JavaScript, deployed on AWS EC2. It crawls, indexes, and queries the Linux Kernel Mailing List (lore.kernel.org/lkml), returning ranked results for keyword searches.

## What it does

- Crawls 100K+ LKML pages across 3 distributed worker nodes
- Indexes documents using a custom MapReduce pipeline with TF-based ranking and 1/2/3-gram support
- Answers keyword queries in ~2ms with ~500 queries/sec throughput

## How it works

Three AWS EC2 nodes act as workers, with a coordinator node orchestrating everything.

**Crawling** — workers fan out from a seed URL, fetch pages, extract links, and store documents in a distributed key-value store sharded by URL hash. The coordinator deduplicates visited URLs.

**Indexing** — a MapReduce job runs over the corpus. Each document gets tokenized, stemmed, and broken into n-grams via a shell pipeline. Map outputs per-document TF scores, shuffle routes terms to their owning nodes, and reduce writes the final inverted index directly to distributed storage.

**Querying** — the query is stemmed the same way, n-grams are looked up in parallel across nodes, scores are summed, and the top 10 results are returned.

## Scale

| Metric | Value |
|--------|-------|
| Documents indexed | 96,388 |
| Unique index terms | ~759,000 |
| Total index size | ~3.1 GB |
| Crawl time | ~4 minutes |
| Index time | ~8 hours |
| Query latency | ~1.96 ms |
| Query throughput | ~506 queries/sec |

## Stack

JavaScript (Node.js), Bash, AWS EC2

