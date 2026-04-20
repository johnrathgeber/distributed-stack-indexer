const coordIP = process.argv[2];
require('./distribution.js')({ip: coordIP, port: 7100});
const distribution = globalThis.distribution;
const id = distribution.util.id;

const n1 = {ip: '172.31.10.12', port: 7110};
const n2 = {ip: '172.31.15.46', port: 7110};
const n3 = {ip: '172.31.6.106', port: 7110};

const group = {};
group[id.getSID(n1)] = n1;
group[id.getSID(n2)] = n2;
group[id.getSID(n3)] = n3;

// function health(node) {
//   distribution.local.comm.send([], {node, service: 'status', method: 'get'}, (e, v) => {
//     if (e) {
//       console.log(`node ${node.ip} might be down?? e: ${e.message}`);
//       return;
//     }
//     const heapUsed = v.heapUsed || 0;
//     const heapTotal = v.heapTotal || 1;
//     console.log(`heap used: ${heapUsed}, heapTotal: ${heapTotal}`);
//   });
// }

const mapper = (key, value) => {
  const {execSync} = require('child_process');
  const fs = require('fs');
  const os = require('os');
  const path = require('path');

  if (!value || !value.url || !value.text) {
    return [];
  }
  const {url, text} = value;

  const BASE = path.join(process.cwd(), 'non-distribution');
  const tmpFile = path.join(os.tmpdir(), `idx_${process.pid}_${Date.now()}`);

  let terms = [];
  try {
    fs.writeFileSync(tmpFile, text, 'utf8');
    const raw = execSync(
      `timeout -k 5 30 bash -c 'cat "${tmpFile}" | "${BASE}/c/process.sh" | node "${BASE}/c/stem.js"'`,
      {cwd: BASE, encoding: 'utf8', timeout: 40000}
    );
    terms = raw.trim().split('\n').filter((t) => t.length > 0);
  } catch (e) {
    return [];
  } finally {
    try {
      fs.unlinkSync(tmpFile);
    } catch (e) {}
  }
  if (terms.length == 0) {
    return [];
  }
  const ngrams = [];
  for (let i = 0; i < terms.length; i++) {
    ngrams.push(terms[i]);
    if (i + 1 < terms.length) {
      ngrams.push(terms[i] + ' ' + terms[i + 1]);
    }
    if (i + 2 < terms.length) {
      ngrams.push(terms[i] + ' ' + terms[i + 1] + ' ' + terms[i + 2]);
    }
  }
  const total = ngrams.length;
  const counts = {};
  for (const ng of ngrams) {
    if (!counts[ng]) {
      counts[ng] = 1;
    }
    else {
      counts[ng] += 1;
    }
  }

  return Object.entries(counts).map(([term, count]) => ({[term]: {[url]: count / total}}));
};

const reducer = (key, values) => {
  const urls = {};
  for (const v of values) {
    for (const [url, tf] of Object.entries(v)) {
      if (!urls[url]) {
        urls[url] = tf;
      }
      else {
        ursl[url] += tf
      }
    }
  }
  return {[key]: urls};
};

distribution.node.start(() => {
  distribution.local.groups.put({gid: 'corpus'}, group, () => {
    distribution.corpus.groups.put({gid: 'corpus'}, group, () => {
      distribution.local.groups.put({gid: 'index'}, group, () => {
        distribution.index.groups.put({gid: 'index'}, group, () => {
          distribution.corpus.store.get(null, (e, keys) => {
            if (!keys || keys.length == 0) {
              process.exit(1);
            }
            console.log(`Indexing ${keys.length} documents...`);

            distribution.corpus.mr.exec({keys, map: mapper, reduce: reducer}, (e, results) => {
              if (e) {
                console.error('mr.exec error:', e);
                process.exit(1);
              }
              console.log(`MapReduce complete!!!`);
              process.exit(0);
            });
          });
        });
      });
    });
  });
});
