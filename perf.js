#!/usr/bin/env node
'use strict';
/**
 * M7 Performance Evaluation
 *
 * Measures latency for 8 RDD workloads across dataset sizes and worker counts.
 * Writes perf-results.json and perf.html when finished.
 *
 * Usage:  node perf.js
 */

// Enable HTTP keep-alive globally so connections are reused rather than
// closed (and entering TIME_WAIT). This prevents ephemeral-port exhaustion
// when running hundreds of back-to-back benchmark requests.
const http = require('http');
http.globalAgent = new http.Agent({keepAlive: true, maxSockets: 64, maxFreeSockets: 16});

require('./distribution.js')();
const dist = globalThis.distribution;
const id = dist.util.id;
const fs = require('fs');

/* ─── Configuration ──────────────────────────────────────────────────────── */

const NODES = [
  {ip: '127.0.0.1', port: 7110},
  {ip: '127.0.0.1', port: 7111},
  {ip: '127.0.0.1', port: 7112},
];

const SIZES = [100, 500, 1000, 2000, 5000];
const WORKER_COUNTS = [1, 2, 3];

/* ─── Utility ────────────────────────────────────────────────────────────── */

/** Run an array of (cb)=>void tasks one at a time. */
function seq(tasks, done) {
  if (tasks.length === 0) { done(); return; }
  tasks[0](() => seq(tasks.slice(1), done));
}

/**
 * Store pairs concurrently with a cap on in-flight operations.
 * Avoids opening thousands of HTTP connections simultaneously.
 */
function putAll(gid, pairs, cb) {
  if (pairs.length === 0) { cb(); return; }
  const CONCURRENCY = 64;
  let next = 0;
  let inflight = 0;
  let finished = 0;

  function dispatch() {
    while (inflight < CONCURRENCY && next < pairs.length) {
      const [k, v] = pairs[next++];
      inflight++;
      dist[gid].store.put(v, k, () => {
        inflight--;
        finished++;
        if (finished === pairs.length) { cb(); return; }
        dispatch();
      });
    }
  }

  dispatch();
}

/** Create a named group with the given nodes on the orchestrator and workers. */
function makeGroup(gid, nodes, cb) {
  const group = {};
  for (const n of nodes) group[id.getSID(n)] = n;
  dist.local.groups.put({gid}, group, () => {
    dist[gid].groups.put({gid}, group, () => cb());
  });
}

/** Call fn(resultCb) and measure wall-clock time until resultCb fires. */
function timeIt(fn, cb) {
  const start = Date.now();
  fn((e, v) => cb(e, Date.now() - start));
}

/* ─── Workload definitions ───────────────────────────────────────────────── */

/**
 * Returns the 8 workloads for a given (gid, size).
 * Each workload is {name, run(cb)}.
 *
 * Key naming:
 *   main dataset : p0 … p{size-1}
 *   join left    : lp0 … lp{size/2-1}
 *   join right   : rp0 … rp{size/2-1}
 *
 * All user functions must be self-contained (eval'd on workers).
 * Size-dependent literals are embedded via eval-built functions.
 */
function makeWorkloads(gid, size) {
  const mainKeys = Array.from({length: size}, (_, i) => `p${i}`);
  const half = Math.floor(size / 2);
  const leftKeys = Array.from({length: half}, (_, i) => `lp${i}`);
  const rightKeys = Array.from({length: half}, (_, i) => `rp${i}`);

  // Join mappers: embed `half` as a numeric literal so the function is
  // self-contained when serialized via fn.toString().
  const leftMapper = eval(`(function(k,v){ return ['j'+String(v%${half}), v]; })`);
  const rightMapper = eval(`(function(k,v){ return ['j'+String(v%${half}), v]; })`);

  return [
    {
      name: 'map',
      run: (cb) => dist[gid].rdd.from(mainKeys)
        .map((k, v) => [k, v * 2])
        .collect(cb),
    },
    {
      name: 'filter',
      run: (cb) => dist[gid].rdd.from(mainKeys)
        .filter((k, v) => v % 2 === 0)
        .collect(cb),
    },
    {
      name: 'flatMap',
      run: (cb) => dist[gid].rdd.from(mainKeys)
        .flatMap((k, v) => [[k, v], [k + '_x', v + 1]])
        .collect(cb),
    },
    {
      name: 'map+filter (fused)',
      run: (cb) => dist[gid].rdd.from(mainKeys)
        .map((k, v) => [k, v * 2])
        .filter((k, v) => v % 4 === 0)
        .collect(cb),
    },
    {
      name: 'reduceByKey',
      run: (cb) => dist[gid].rdd.from(mainKeys)
        .map((k, v) => [String(v % 10), 1])
        .reduceByKey((a, b) => a + b)
        .collect(cb),
    },
    {
      name: 'groupByKey',
      run: (cb) => dist[gid].rdd.from(mainKeys)
        .map((k, v) => [String(v % 10), v])
        .groupByKey()
        .collect(cb),
    },
    {
      name: 'sortByKey',
      run: (cb) => dist[gid].rdd.from(mainKeys)
        .sortByKey()
        .collect(cb),
    },
    {
      name: 'join',
      run: (cb) => {
        const lRdd = dist[gid].rdd.from(leftKeys).map(leftMapper);
        const rRdd = dist[gid].rdd.from(rightKeys).map(rightMapper);
        lRdd.join(rRdd).collect(cb);
      },
    },
  ];
}

/* ─── Benchmark runner ───────────────────────────────────────────────────── */

const RESULTS = [];

function runSize(gid, size, wcnt, cb) {
  // Store main + join datasets once; run all workloads against them.
  const mainPairs = Array.from({length: size}, (_, i) => [`p${i}`, i]);
  const half = Math.floor(size / 2);
  const leftPairs = Array.from({length: half}, (_, i) => [`lp${i}`, i]);
  const rightPairs = Array.from({length: half}, (_, i) => [`rp${i}`, i]);

  process.stdout.write(`  storing ${size} records... `);
  putAll(gid, mainPairs, () => {
    putAll(gid, leftPairs, () => {
      putAll(gid, rightPairs, () => {
        process.stdout.write('stored\n');

        const workloads = makeWorkloads(gid, size);
        seq(workloads.map((wl) => (next) => {
          process.stdout.write(`    [w=${wcnt} n=${size}] ${wl.name} ... `);
          timeIt(wl.run, (e, ms) => {
            if (e) {
              process.stdout.write(`ERROR: ${e.message}\n`);
            } else {
              process.stdout.write(`${ms} ms\n`);
              RESULTS.push({workload: wl.name, workers: wcnt, size, ms});
            }
            next();
          });
        }), cb);
      });
    });
  });
}

/** Stop workers (ignoring errors from already-dead processes). */
function stopWorkers(nodes, cb) {
  seq(nodes.map((node) => (next) =>
    dist.local.comm.send([], {node, service: 'status', method: 'stop'}, () => next())
  ), cb);
}

function runWorkerCount(wcnt, cb) {
  const gid = `perf${wcnt}`;
  const nodes = NODES.slice(0, wcnt);
  console.log(`\n=== ${wcnt} worker${wcnt > 1 ? 's' : ''} ===`);
  seq(SIZES.map((sz) => (next) => runSize(gid, sz, wcnt, next)), cb);
}

/* ─── HTML report generator ──────────────────────────────────────────────── */

function generateHTML(results) {
  const workloadNames = [...new Set(results.map((r) => r.workload))];
  const sizes = SIZES;
  const COLORS = ['#e74c3c', '#2ecc71', '#3498db'];
  const LABELS = ['1 worker', '2 workers', '3 workers'];

  const canvases = workloadNames.map((wl, i) => `
    <div class="card">
      <h3>${wl}</h3>
      <canvas id="c${i}"></canvas>
    </div>`).join('\n');

  const scripts = workloadNames.map((wl, i) => {
    const datasets = WORKER_COUNTS.map((wc, wi) => {
      const data = sizes.map((sz) => {
        const r = results.find((r) => r.workload === wl && r.workers === wc && r.size === sz);
        return r ? r.ms : null;
      });
      return {
        label: LABELS[wi],
        data,
        borderColor: COLORS[wi],
        backgroundColor: COLORS[wi] + '33',
        fill: false,
        tension: 0.2,
        pointRadius: 4,
      };
    });

    return `new Chart(document.getElementById('c${i}'), {
      type: 'line',
      data: { labels: ${JSON.stringify(sizes)}, datasets: ${JSON.stringify(datasets)} },
      options: {
        responsive: true,
        plugins: { legend: { position: 'top' } },
        scales: {
          x: { title: { display: true, text: 'Dataset size (records)' } },
          y: { title: { display: true, text: 'Latency (ms)' }, beginAtZero: true }
        }
      }
    });`;
  }).join('\n');

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>M7 RDD Performance Report</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: system-ui, sans-serif; background: #f5f5f5; padding: 24px; }
    h1  { text-align: center; margin-bottom: 6px; }
    .subtitle { text-align: center; color: #666; margin-bottom: 24px; font-size: 0.9rem; }
    .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(480px, 1fr)); gap: 20px; }
    .card { background: #fff; border-radius: 8px; padding: 20px;
            box-shadow: 0 1px 4px rgba(0,0,0,.1); }
    .card h3 { font-size: 1rem; margin-bottom: 12px; text-transform: capitalize; color: #333; }
  </style>
</head>
<body>
  <h1>M7 RDD Performance Report</h1>
  <p class="subtitle">
    Latency (ms) vs. dataset size across 8 workloads and 1–3 workers |
    Generated ${new Date().toISOString().slice(0, 16).replace('T', ' ')} UTC
  </p>
  <div class="grid">
${canvases}
  </div>
  <script>
${scripts}
  </script>
</body>
</html>`;

  fs.writeFileSync('perf.html', html);
}

/* ─── Entry point ────────────────────────────────────────────────────────── */

console.log('M7 RDD Performance Evaluation');
console.log(`Workloads: 8   Sizes: ${SIZES.join(', ')}   Worker counts: ${WORKER_COUNTS.join(', ')}`);
console.log('Starting distribution node...');

dist.node.start(() => {
  console.log('Spawning workers...');
  // Spawn all 3 workers once; groups of different sizes share the same workers.
  dist.local.status.spawn(NODES[0], () => {
    dist.local.status.spawn(NODES[1], () => {
      dist.local.status.spawn(NODES[2], () => {
        console.log('Workers ready. Creating groups...');

        // Pre-create perf1 / perf2 / perf3 groups before benchmarks start.
        seq(WORKER_COUNTS.map((wc) => (next) => makeGroup(`perf${wc}`, NODES.slice(0, wc), next)), () => {
          console.log('Groups ready. Beginning benchmarks...');

          seq(WORKER_COUNTS.map((wc) => (next) => runWorkerCount(wc, next)), () => {
            // Write raw results
            fs.writeFileSync('perf-results.json', JSON.stringify(RESULTS, null, 2));
            console.log('\nResults written to perf-results.json');

            // Generate HTML report
            generateHTML(RESULTS);
            console.log('Report written to perf.html');

            // Shutdown workers
            stopWorkers(NODES, () => {
              if (globalThis.distribution.node.server) {
                globalThis.distribution.node.server.close();
              }
              process.exit(0);
            });
          });
        });
      });
    });
  });
});
