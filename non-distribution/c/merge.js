#!/usr/bin/env node

/*
Merge the current inverted index (assuming the right structure) with the global index file
Usage: input > ./merge.js global-index > output

The inverted indices have the different structures!

Each line of a local index is formatted as:
  - `<word/ngram> | <frequency> | <url>`

Each line of a global index is be formatted as:
  - `<word/ngram> | <url_1> <frequency_1> <url_2> <frequency_2> ... <url_n> <frequency_n>`
  - Where pairs of `url` and `frequency` are in descending order of frequency
  - Everything after `|` is space-separated

-------------------------------------------------------------------------------------
Example:

local index:
  word1 word2 | 8 | url1
  word3 | 1 | url9
EXISTING global index:
  word1 word2 | url4 2
  word3 | url3 2

merge into the NEW global index:
  word1 word2 | url1 8 url4 2
  word3 | url3 2 url9 1

Remember to error gracefully, particularly when reading the global index file.
*/

const fs = require('fs');
const readline = require('readline');
// The `compare` function can be used for sorting.
const compare = (a, b) => {
  if (a.freq > b.freq) {
    return -1;
  } else if (a.freq < b.freq) {
    return 1;
  } else {
    return 0;
  }
};
const rl = readline.createInterface({
  input: process.stdin,
});

// 1. Read the incoming local index data from standard input (stdin) line by line.
let localIndex = '';
rl.on('line', (line) => {
  localIndex += line + '\n';
});

rl.on('close', () => {
  // 2. Read the global index name/location, using process.argv
  // and call printMerged as a callback
  if (process.argv.length < 3) {
    console.error('Error: No global index file provided.');
    process.exit(1);
  }
  fs.readFile(process.argv[2], 'utf8', printMerged);
});

const printMerged = (err, data) => {
  if (err) {
    if (err.code === 'ENOENT') {
      data = '';
    } else {
      console.error('Error reading file:', err);
      return;
    }
  }

  // Split the data into an array of lines
  const localIndexLines = localIndex.split('\n');
  const globalIndexLines = data.split('\n');

  localIndexLines.pop();
  globalIndexLines.pop();

  const local = {};
  const global = {};

  // 3. For each line in `localIndexLines`, parse them and add them to the `local` object
  // where keys are terms and values store a url->freq map (one entry per url).
  for (const line of localIndexLines) {
    const words = line.split(' | ');
    if (words.length < 3) {
      console.error('Invalid local index.');
      process.exit(1);
    }
    if (isNaN(words[1])) {
      console.error('Invalid local index.');
      process.exit(1);
    }
    local[words[0]] = {[words[2]]: parseInt(words[1])};
  }

  // 4. For each line in `globalIndexLines`, parse them and add them to the `global` object
  // where keys are terms and values are url->freq maps (one entry per url).
  // Use the .trim() method to remove leading and trailing whitespace from a string.
  for (const line of globalIndexLines) {
    const words = line.trim().split(' | ');
    if (words.length < 2) {
      continue;
      // console.error('Invalid global index.', {globalIndexLines});
      // process.exit(1);
    }
    const term = words[0];
    const urlFreq = words[1].split(' ');
    const grouped = {};
    if (urlFreq.length % 2 != 0) {
      console.error('Invalid global index.');
      process.exit(1);
    }
    for (let i = 0; i < urlFreq.length; i += 2) {
      if (isNaN(urlFreq[i + 1])) {
        console.error('Invalid global index.');
        process.exit(1);
      }
      grouped[urlFreq[i]] = parseInt(urlFreq[i + 1]);
    }
    global[term] = grouped; // Map<url, freq>
  }

  // 5. Merge the local index into the global index:
  // - For each term in the local index, if the term exists in the global index:
  //     - Merge by url so there is at most one entry per url.
  //     - Sum frequencies for duplicate urls.
  // - If the term does not exist in the global index:
  //     - Add it as a new entry with the local index's data.
  // 6. Print the merged index to the console in the same format as the global index file:
  //    - Each line contains a term, followed by a pipe (`|`), followed by space-separated pairs of `url` and `freq`.
  //    - Terms should be printed in alphabetical order.
  for (const [term, mp] of Object.entries(local)) {
    if (term in global) {
      const url = Object.keys(mp)[0];
      if (url in global[term]) {
        global[term][url] += mp[url];
      } else {
        global[term][url] = mp[url];
      }
    } else {
      global[term] = mp;
    }
  }
  const terms = Object.keys(global).sort();
  for (const term of terms) {
    const urlMap = global[term];
    const entries = [];
    for (const [url, freq] of Object.entries(urlMap)) {
      entries.push({url: url, freq: freq});
    }
    entries.sort(compare);
    const s = entries.map((e) => `${e.url} ${e.freq}`).join(' ');
    console.log(`${term} | ${s}`);
  }
};
