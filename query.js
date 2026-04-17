const coordIP = process.argv[2] || '127.0.0.1';
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

const args = process.argv.slice(3);

distribution.node.start(() => {
  distribution.local.groups.put({gid: 'index'}, group, () => {
    distribution.index.groups.put({gid: 'index'}, group, () => {
      const {execSync} = require('child_process');
      const path = require('path');
      const BASE = path.join(process.cwd(), 'non-distribution');

      const raw = execSync(
        `echo "${args.join(' ')}" | "${BASE}/c/process.sh" | node "${BASE}/c/stem.js"`,
        {cwd: BASE, encoding: 'utf8'}
      );
      const stems = raw.trim().split('\n').filter(Boolean);

      const lookupTerms = new Set();
      for (let i = 0; i < stems.length; i++) {
        lookupTerms.add(stems[i]);
        if (i + 1 < stems.length) {
          lookupTerms.add(stems[i] + ' ' + stems[i + 1]);
        }
        if (i + 2 < stems.length) {
          lookupTerms.add(stems[i] + ' ' + stems[i + 1] + ' ' + stems[i + 2]);
        }
      }

      const terms = [...lookupTerms];
      let pending = terms.length;
      const scores = {};

      for (const term of terms) {
        distribution.index.store.get(id.getID(term.replace(/[^a-zA-Z0-9]/g, '')), (e, urlMap) => {
          if (urlMap) {
            for (const [url, score] of Object.entries(urlMap)) {
              scores[url] = (scores[url] || 0) + score;
            }
          }
          pending--;
          if (pending == 0) {
            const ranked = Object.entries(scores)
              .sort((a, b) => b[1] - a[1])
              .slice(0, 10);

            ranked.forEach(([url, score], i) => {
              console.log(`${i + 1}. ${url} (${score.toFixed(4)})`);
            });
            process.exit(0);
          }
        });
      }
    });
  });
});
