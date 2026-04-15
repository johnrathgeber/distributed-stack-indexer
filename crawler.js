const coordIP = process.argv[2] || '127.0.0.1';
require('./distribution.js')({ip: coordIP, port: 7100});
const distribution = globalThis.distribution;
const id = distribution.util.id;

const SEED = 'https://lore.kernel.org/lkml/';
const TARGET = 100000;

const n1 = {ip: '172.31.10.12', port: 7110};
const n2 = {ip: '172.31.15.46', port: 7110};
const n3 = {ip: '172.31.6.106', port: 7110};

const group = {};
group[id.getSID(n1)] = n1;
group[id.getSID(n2)] = n2;
group[id.getSID(n3)] = n3;

const visited = new Set();

function crawlIt(callback) {
  const https = require('https');
  const DOMAIN = 'https://lore.kernel.org/lkml/';

  globalThis.distribution.local.store.get({key: null, gid: 'frontier'}, (e, keys) => {
    if (!keys || keys.length == 0) {
      return callback(null, []);
    }

    let kpending = keys.length;
    const pairs = [];

    for (const key of keys) {
      globalThis.distribution.local.store.get({key, gid: 'frontier'}, (e2, url) => {
        if (!e2 && url) {
          pairs.push({key, url});
        }
        kpending--;
        if (kpending == 0) {
          if (pairs.length == 0) {
            return callback(null, []);
          }

          let pending = pairs.length;
          const links = [];

          const done = () => {
            pending--;
            if (pending == 0) {
              callback(null, links);
            }
          };

          for (const {key, url} of pairs) {
            globalThis.distribution.local.store.del({key, gid: 'frontier'}, () => {});

            const fetch = (u, hops) => {
              if (hops > 3) {
                return done();
              }
              try {
                const req = https.get(u, {headers: {'User-Agent': 'axios/1.6.0'}, timeout: 10000}, res => {
                  if (res.statusCode >= 300 && res.headers.location) {
                    try {
                      fetch(new URL(res.headers.location, u).href, hops + 1);
                    } catch (e) {
                      done();
                    }
                    return;
                  }
                  let html = '';
                  res.on('data', d => { html += d; });
                  res.on('end', () => {
                    const text = html
                      .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '')
                      .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '')
                      .replace(/<[^>]+>/g, ' ')
                      .replace(/\s+/g, ' ').trim();

                    const corpusKey = globalThis.distribution.util.id.getID(url);
                    globalThis.distribution.corpus.store.put({url, text}, corpusKey, () => {});

                    const re = /href="([^"]+)"/g;
                    let m;
                    while ((m = re.exec(html)) !== null) {
                      try {
                        const abs = new URL(m[1], u).href;
                        if (abs.startsWith(DOMAIN)) {
                          links.push(abs);
                        }
                      } catch (e) {}
                    }
                    done();
                  });
                  res.on('error', () => done());
                });
                req.on('error', () => done());
                req.on('timeout', () => {
                  req.destroy();
                  done();
                });
              } catch (e) {
                done();
              }
            };
            fetch(url, 0);
          }
        }
      });
    }
  });
}

function putUrls(urls, cb) {
  if (!urls.length) {
    return cb();
  }
  let n = urls.length;
  for (const url of urls) {
    const key = id.getID(url);
    distribution.frontier.store.put(url, key, () => {
      if (--n == 0) {
        cb();
      }
    });
  }
}

function runRound(cb) {
  let n = 3;
  let allLinks = [];
  for (const node of [n1, n2, n3]) {
    distribution.local.comm.send(
      [], {node, service: 'crawler', method: 'crawlIt'},
      (e, links) => {
        if (links) {
          allLinks = allLinks.concat(links);
        }
        if (--n == 0) {
          cb(allLinks);
        }
      }
    );
  }
}

function loop() {
  console.log(`visited=${visited.size}`);
  if (visited.size >= TARGET) {
    console.log('Done crawling.');
    process.exit(0);
  }

  runRound(links => {
    const newUrls = [...new Set(links)].filter(u => !visited.has(u));
    newUrls.forEach(u => visited.add(u));
    putUrls(newUrls, loop);
  });
}

distribution.node.start(() => {
  distribution.local.groups.put({gid: 'frontier'}, group, () => {
    distribution.frontier.groups.put({gid: 'frontier'}, group, () => {
      distribution.local.groups.put({gid: 'corpus'}, group, () => {
        distribution.corpus.groups.put({gid: 'corpus'}, group, () => {
          let n = 3;
          for (const node of [n1, n2, n3]) {
            distribution.local.comm.send([{crawlIt}, 'crawler'],
              {node, service: 'routes', method: 'put'},
              () => {
                if (--n == 0) {
                  visited.add(SEED);
                  putUrls([SEED], loop);
                }
              }
            );
          }
        });
      });
    });
  });
});
