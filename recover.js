const coordIP = process.argv[2] || '127.0.0.1';
const SERV_NAME = process.argv[3];
if (!SERV_NAME) {
  console.error('Usage: node recover.js <coordIP> <servName>');
  console.error('  e.g. node recover.js 172.31.10.12 mr-vgPxVh9');
  process.exit(1);
}

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

const reducer = (key, values) => {
  const urls = {};
  for (const v of values) {
    for (const [url, tf] of Object.entries(v)) {
      urls[url] = (urls[url] || 0) + tf;
    }
  }
  return {[key]: urls};
};

distribution.node.start(() => {
  distribution.local.groups.put({gid: 'corpus'}, group, () => {
    distribution.corpus.groups.put({gid: 'corpus'}, group, () => {
      distribution.local.groups.put({gid: 'index'}, group, () => {
        distribution.index.groups.put({gid: 'index'}, group, () => {
          const nodes = Object.values(group);
          const coordNode = distribution.node.config;
          const dummyConfig = {map: () => [], reduce: reducer, keys: []};

          let notifyCntr = 0;
          const notifyServ = {
            notify: (cb) => {
              cb(null, {});
              notifyCntr++;
              if (notifyCntr < nodes.length) { return; }

              console.log('Workers ready. Running recovery reduce...');
              let doneCntr = 0;
              for (const node of nodes) {
                distribution.local.comm.send(
                  [SERV_NAME],
                  {node, service: 'mr', method: 'recoverReduce'},
                  (e) => {
                    if (e) { console.error('recoverReduce error:', e); process.exit(1); }
                    doneCntr++;
                    if (doneCntr === nodes.length) {
                      console.log('Recovery complete. Index stored.');
                      process.exit(0);
                    }
                  }
                );
              }
            }
          };

          distribution.local.routes.put(notifyServ, SERV_NAME, () => {
            for (const node of nodes) {
              distribution.local.comm.send(
                [dummyConfig, coordNode, SERV_NAME, 'corpus'],
                {node, service: 'mr', method: 'recvMapReduce'},
                (e) => {
                  if (e) { console.error('recvMapReduce error:', e); process.exit(1); }
                }
              );
            }
          });
        });
      });
    });
  });
});
