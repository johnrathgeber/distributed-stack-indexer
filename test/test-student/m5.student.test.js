/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Imporant: Do not modify any of the test headers (i.e., the test('header', ...) part). Doing so will result in grading penalties.
*/

const distribution = require('../../distribution.js')();
const id = require('../../distribution/util/id.js');
require('../helpers/sync-guard');

const test1Group = {};
const test2Group = {};
const test3Group = {};
const test4Group = {};
const test5Group = {};
const test6Group = {};

const n1 = {ip: '127.0.0.1', port: 7110};
const n2 = {ip: '127.0.0.1', port: 7111};
const n3 = {ip: '127.0.0.1', port: 7112};

test('(1 pts) student test', (done) => {
  const mapper = (key, value) => {
    const out = [];
    value.forEach((w) => {
      const o = {};
      o[w] = [w];
      out.push(o);
    });
    return out;
  };

  const reducer = (key, values) => {
    const out = {};
    out[key] = values.reduce((a, b) => a.concat(b), []);
    return out;
  };

  const dataset = [
    {'b1-l1': ["hi", "it"]},
    {'b1-l2': ["is", "nice", "to", "meet", "you", "hi"]},
  ];

  const expected = [
    {'hi': ["hi", "hi"]},
    {'it': ["it"]},
    {'is': ["is"]},
    {'nice': ["nice"]},
    {'to': ["to"]},
    {'meet': ["meet"]},
    {'you': ["you"]},
  ];

  const doMapReduce = () => {
    distribution.test1.store.get(null, (e, v) => {
      try {
        expect(v.length).toEqual(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.test1.mr.exec({keys: v, map: mapper, reduce: reducer}, (e, v) => {
        try {
          expect(v).toEqual(expect.arrayContaining(expected));
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // Send the dataset to the cluster
  dataset.forEach((o) => {
    const key = Object.keys(o)[0];
    const value = o[key];
    distribution.test1.store.put(value, key, (e, v) => {
      cntr++;
      // Once the dataset is in place, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});


test('(1 pts) student test', (done) => {
  const mapper = (key, value) => {
    const out = [];
    const o = {};
    o["key"] = value + "hi";
    out.push(o);
    return out;
  };

  const reducer = (key, values) => {
    const out = {};
    out[key] = values.reduce((a, b) => a.concat(b), []);
    return out;
  };

  const dataset = [
    {'b1-l1': "athing1"},
    {'b1-l2': "athing2"},
  ];

  const expected = [
    {'key': ["athing1hi", "athing2hi"]},
    {'key': ["athing2hi", "athing1hi"]},
  ];

  const doMapReduce = () => {
    distribution.test2.store.get(null, (e, v) => {
      try {
        expect(v.length).toEqual(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.test2.mr.exec({keys: v, map: mapper, reduce: reducer}, (e, v) => {
        try {
          expect(expected).toEqual(expect.arrayContaining(v));
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // Send the dataset to the cluster
  dataset.forEach((o) => {
    const key = Object.keys(o)[0];
    const value = o[key];
    distribution.test2.store.put(value, key, (e, v) => {
      cntr++;
      // Once the dataset is in place, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});


test('(1 pts) student test', (done) => {
  const mapper = (key, value) => {
    const out = [];
    const o = {};
    o[Number(key) % 5] = value + "hi";
    out.push(o);
    return out;
  };

  const reducer = (key, values) => {
    const out = {};
    out[key] = (values.reduce((a, b) => a.concat(b), [])).sort();
    return out;
  };

  const dataset = [
    {'1': "athing1"},
    {'2': "athing2"},
    {'3': "athing3"},
    {'4': "athing4"},
    {'5': "athing5"},
    {'6': "athing6"},
    {'7': "athing7"},
    {'8': "athing8"},
    {'9': "athing9"},
    {'10': "athing10"},
    {'11': "athing11"},
    {'12': "athing12"},
    {'13': "athing13"},
    {'14': "athing14"},
    {'15': "athing15"},
    {'16': "athing16"},
    {'17': "athing17"},
    {'18': "athing18"},
    {'19': "athing19"},
    {'20': "athing20"},
  ];

  const expected = [
    {'0': (["athing5hi", "athing10hi", "athing15hi", "athing20hi"]).sort()},
    {'1': (["athing1hi", "athing6hi", "athing11hi", "athing16hi"]).sort()},
    {'2': (["athing2hi", "athing7hi", "athing12hi", "athing17hi"]).sort()},
    {'3': (["athing3hi", "athing8hi", "athing13hi", "athing18hi"]).sort()},
    {'4': (["athing4hi", "athing9hi", "athing14hi", "athing19hi"]).sort()},
  ];

  const doMapReduce = () => {
    distribution.test3.store.get(null, (e, v) => {
      try {
        expect(v.length).toEqual(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.test3.mr.exec({keys: v, map: mapper, reduce: reducer}, (e, v) => {
        try {
          expect(v).toEqual(expect.arrayContaining(expected));
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // Send the dataset to the cluster
  dataset.forEach((o) => {
    const key = Object.keys(o)[0];
    const value = o[key];
    distribution.test3.store.put(value, key, (e, v) => {
      cntr++;
      // Once the dataset is in place, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test('(1 pts) student test', (done) => {
  const mapper = (key, value) => {
    const out = [];
    value.forEach((w) => {
      const o = {};
      o[w[0]] = 1;
      out.push(o);
    });
    return out;
  };

  const reducer = (key, values) => {
    const out = {};
    out[key] = values.reduce((a, b) => a + b, 0);
    return out;
  };

  const dataset = [
    {'b1-l1': ["hi", "it"]},
    {'b1-l2': ["is", "nice", "to", "meet", "you", "hi"]},
  ];

  const expected = [
    {'h': 2},
    {'i': 2},
    {'n': 1},
    {'t': 1},
    {'m': 1},
    {'y': 1},
  ];

  const doMapReduce = () => {
    distribution.test4.store.get(null, (e, v) => {
      try {
        expect(v.length).toEqual(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.test4.mr.exec({keys: v, map: mapper, reduce: reducer}, (e, v) => {
        try {
          expect(v).toEqual(expect.arrayContaining(expected));
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // Send the dataset to the cluster
  dataset.forEach((o) => {
    const key = Object.keys(o)[0];
    const value = o[key];
    distribution.test4.store.put(value, key, (e, v) => {
      cntr++;
      // Once the dataset is in place, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test('(1 pts) student test', (done) => {
  const mapper = (key, value) => {
    const out = [];
    const o = {};
    o[key[0]] = typeof value === "number" ? value + 1 : Number(value) * 3;
    out.push(o);
    return out;
  };

  const reducer = (key, values) => {
    const out = {};
    out[key] = values.reduce((a, b) => a * b, 1);
    return out;
  };

  const dataset = [
    {"key": 1},
    {"k2": 2},
    {"dkey": "3"},
    {"anotherkey": "4"},
  ];

  const expected = [
    {"k": 6},
    {"d": 9},
    {"a": 12},
  ];

  const doMapReduce = () => {
    distribution.test5.store.get(null, (e, v) => {
      try {
        expect(v.length).toEqual(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.test5.mr.exec({keys: v, map: mapper, reduce: reducer}, (e, v) => {
        try {
          expect(v).toEqual(expect.arrayContaining(expected));
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // Send the dataset to the cluster
  dataset.forEach((o) => {
    const key = Object.keys(o)[0];
    const value = o[key];
    distribution.test5.store.put(value, key, (e, v) => {
      cntr++;
      // Once the dataset is in place, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

// test('performance', (done) => {
//   const mapper = (key, value) => {
//     const words = value.split(/(\s+)/).filter((e) => e !== ' ');
//     const totalWords = words.length;
//     const wordCounts = {};
//     words.forEach((w) => {
//       if (!wordCounts[w]) {
//         wordCounts[w] = 0;
//       }
//       wordCounts[w]++;
//     });
//     const out = [];
//     for (const w in wordCounts) {
//       const o = {};
//       o[w] = {[key]: wordCounts[w] / totalWords};
//       out.push(o);
//     }
//     return out;
//   };

//   // Reduce function: calculate TF-IDF for each word
//   const reducer = (key, values) => {
//     const totalDocs = 3;
//     let docCount = 0;
//     const docsFound = {};
//     values.forEach((v) => {
//       for (const doc in v) {
//         if (!docsFound[doc]) {
//           docsFound[doc] = true;
//           docCount++;
//         }
//       }
//     });
//     const idf = Math.log10(totalDocs / docCount);
//     const out = {};
//     out[key] = {};
//     values.forEach((v) => {
//       for (const doc in v) {
//         out[key][doc] = Math.round(v[doc] * idf * 100) / 100;
//       }
//     });
//     return out;
//   };

//   const dataset = [
//     {'doc1': 'machine learning is amazing'},
//     {'doc2': 'deep learning powers amazing systems'},
//     {'doc3': 'machine learning and deep learning are related'},
//   ];

//   const expected = [{'is': {'doc1': 0.12}},
//     {'deep': {'doc2': 0.04, 'doc3': 0.03}},
//     {'systems': {'doc2': 0.1}},
//     {'learning': {'doc1': 0, 'doc2': 0, 'doc3': 0}},
//     {'amazing': {'doc1': 0.04, 'doc2': 0.04}},
//     {'machine': {'doc1': 0.04, 'doc3': 0.03}},
//     {'are': {'doc3': 0.07}}, {'powers': {'doc2': 0.1}},
//     {'and': {'doc3': 0.07}}, {'related': {'doc3': 0.07}}];

//   const doMapReduce = () => {
//     distribution.test6.store.get(null, (e, v) => {
//       try {
//         expect(v.length).toEqual(dataset.length);
//       } catch (e) {
//         done(e);
//         return;
//       }
//       const iters = 100;
//       let completed = 0;
//       const startTime = performance.now();
//       function runIter() {
//         distribution.test6.mr.exec({keys: v, map: mapper, reduce: reducer}, (e, v) => {
//           completed++;
//           if (completed < iters) {
//             runIter();
//           }
//           else {
//             const elapsed = performance.now() - startTime;
//             const avgLatency = (elapsed / (iters * 1000));
//             const throughput = (iters / (elapsed / 1000));
//             console.log(`avg latency=${avgLatency}s throughput=${throughput} keys/sec`);
//             done();
//           }
//         });
//       }
//       runIter();
//     });
//   };

//   let cntr = 0;

//   // Send the dataset to the cluster
//   dataset.forEach((o) => {
//     const key = Object.keys(o)[0];
//     const value = o[key];
//     distribution.test6.store.put(value, key, (e, v) => {
//       cntr++;
//       // Once the dataset is in place, run the map reduce
//       if (cntr === dataset.length) {
//         doMapReduce();
//       }
//     });
//   });
// });

beforeAll((done) => {
  test1Group[id.getSID(n1)] = n1;
  test1Group[id.getSID(n2)] = n2;
  test1Group[id.getSID(n3)] = n3;

  test2Group[id.getSID(n1)] = n1;
  test2Group[id.getSID(n2)] = n2;
  test2Group[id.getSID(n3)] = n3;

  test3Group[id.getSID(n1)] = n1;
  test3Group[id.getSID(n2)] = n2;
  test3Group[id.getSID(n3)] = n3;

  test4Group[id.getSID(n1)] = n1;
  test4Group[id.getSID(n2)] = n2;
  test4Group[id.getSID(n3)] = n3;

  test5Group[id.getSID(n1)] = n1;
  test5Group[id.getSID(n2)] = n2;
  test5Group[id.getSID(n3)] = n3;

  test6Group[id.getSID(n1)] = n1;
  test6Group[id.getSID(n2)] = n2;
  test6Group[id.getSID(n3)] = n3;

  const startNodes = (cb) => {
    distribution.local.status.spawn(n1, (e, v) => {
      distribution.local.status.spawn(n2, (e, v) => {
        distribution.local.status.spawn(n3, (e, v) => {
          cb();
        });
      });
    });
  };

  distribution.node.start(() => {
    const test1Config = {gid: 'test1'};
    startNodes(() => {
      distribution.local.groups.put(test1Config, test1Group, (e, v) => {
        distribution.test1.groups.put(test1Config, test1Group, (e, v) => {
          const test2Config = {gid: 'test2'};
          distribution.local.groups.put(test2Config, test2Group, (e, v) => {
            distribution.test2.groups.put(test2Config, test2Group, (e, v) => {
              const test3Config = {gid: 'test3'};
              distribution.local.groups.put(test3Config, test3Group, (e, v) => {
                distribution.test3.groups.put(test3Config, test3Group, (e, v) => {
                  const test4Config = {gid: 'test4'};
                  distribution.local.groups.put(test4Config, test4Group, (e, v) => {
                    distribution.test4.groups.put(test4Config, test4Group, (e, v) => {
                      const test5Config = {gid: 'test5'};
                      distribution.local.groups.put(test5Config, test5Group, (e, v) => {
                        distribution.test5.groups.put(test5Config, test5Group, (e, v) => {
                          const test6Config = {gid: 'test6'};
                          distribution.local.groups.put(test6Config, test6Group, (e, v) => {
                            distribution.test6.groups.put(test6Config, test6Group, (e, v) => {
                              done();
                            });
                          });
                        });
                      });
                    });
                  });
                });
              });
            });
          });
        });
      });
    });
  });
});

afterAll((done) => {
  const remote = {service: 'status', method: 'stop'};
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        if (globalThis.distribution.node.server) {
          globalThis.distribution.node.server.close();
        }
        done();
      });
    });
  });
});
