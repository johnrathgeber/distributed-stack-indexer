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

const QUERIES = ['linux kernel', 'memory management', 'file system', 'scheduler', 'network driver',
  'patch', 'interrupt', 'syscall', 'buffer', 'fault'
];
const N = 20;

distribution.node.start(() => {
  distribution.local.groups.put({gid: 'index'}, group, () => {
    distribution.index.groups.put({gid: 'index'}, group, () => {
      const latencies = [];
      let queryIdx = 0;

      function runNext() {
        if (queryIdx == QUERIES.length * N) {
          const avg = latencies.reduce((a, b) => a + b, 0) / latencies.length;
          const throughput = 1000 / avg;
          console.log(`Queries run: ${latencies.length}`);
          console.log(`Avg latency: ${avg.toFixed(2)} ms`);
          console.log(`Throughput:  ${throughput.toFixed(2)} queries/sec`);
          process.exit(0);
          return;
        }
        const term = QUERIES[queryIdx % QUERIES.length];
        queryIdx++;
        const key = id.getID(term.replace(/[^a-zA-Z0-9]/g, ''));
        const start = performance.now();
        distribution.index.store.get(key, (e, v) => {
          latencies.push(performance.now() - start);
          runNext();
        });
      }
      runNext();
    });
  });
});
