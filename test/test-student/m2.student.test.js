/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Imporant: Do not modify any of the test headers (i.e., the test('header', ...) part). Doing so will result in grading penalties.
*/

// const { PerformanceNodeEntry } = require('node:perf_hooks');
// const test = require('node:test');

const distribution = require('../../distribution.js')();
require('../helpers/sync-guard');

test('(1 pts) student test', (done) => {
  // Fill out this test case...
  distribution.node.start(() => {
    distribution.local.status.get("counts", (e, v) => {
    try {
      expect(e).toBeFalsy();
      expect(v).toBe(0);
      const node = distribution.node.config;
      const remote = {node: node, service: 'status', method: 'get'};
      const message = ['nid'];

      distribution.local.comm.send(message, remote, (e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v).toEqual(distribution.util.id.getNID(node));
          distribution.local.status.get("counts", (e, v) => {
            try {
              expect(e).toBeFalsy();
              expect(v).toBe(1);
              done();
            }
            catch (e) {
              done(e);
            }
          });
        }
        catch (e) {
          done(e);
        }
      });
    }
    catch (e) {
      done(e);
    }
    });
  });
});


test('(1 pts) student test', (done) => {
  // Fill out this test case...
  distribution.local.routes.get({}, (e, v) => {
    try {
      expect(e).toBeDefined();
      expect(e).toBeInstanceOf(Error);
      expect(v).toBeFalsy();
      distribution.local.routes.get({"service": "a"}, (e, v) => {
        try {
          expect(e).toBeDefined();
          expect(e).toBeInstanceOf(Error);
          expect(v).toBeFalsy();
          done();
        }
        catch (e) {
          done(e);
        }
      });
    } catch (e) {
      done(e);
    }
  });
});


test('(1 pts) student test', (done) => {
  // Fill out this test case...
  distribution.local.routes.put("a", "aardvark", (e, v) => {
    try {
      expect(e).toBeFalsy();
      expect(v).toBe(null);
      distribution.local.routes.put("b", "balloon", (e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v).toBe(null);
          distribution.local.routes.get("aardvark", (e, v) => {
            try {
              expect(e).toBeFalsy();
              expect(v).toBe("a");
              distribution.local.routes.get("b", (e, v) => {
                try {
                  expect(e).toBeDefined();
                  expect(e).toBeInstanceOf(Error);
                  expect(v).toBeFalsy();
                  done();
                }
                catch (e) {
                  done(e);
                }
              });
            }
            catch (e) {
              done(e);
            }
          });
        } catch (e) {
          done(e);
        }
      });
    } catch (e) {
      done(e);
    }
  });
});

test('(1 pts) student test', (done) => {
  // Fill out this test case...
  distribution.local.routes.put("c", "chameleon", (e, v) => {
    try {
      expect(e).toBeFalsy();
      expect(v).toBe(null);
      distribution.local.routes.put("d", "darwin", (e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v).toBe(null);
          distribution.local.routes.rem("c", (e, v) => {
            try {
              expect(e).toBeDefined();
              expect(e).toBeInstanceOf(Error);
              expect(v).toBeFalsy();
              distribution.local.routes.rem("darwin", (e, v) => {
                try {
                  expect(e).toBeFalsy();
                  expect(v).toBe("d");
                  done();
                } catch (e) {
                  done(e);
                }
              });
            } catch (e) {
              done(e);
            }
          });
        } catch (e) {
          done(e);
        }
      });
    } catch (e) {
      done(e);
    }
  });
});

test('(1 pts) student test', (done) => {
  const node = distribution.node.config;
  const options = {
    hostname: node.ip,
    port: node.port,
    path: '/local/routes/put',
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
  };
  const http = require('node:http');
  const request = http.request(options, (response) => {
    let data = '';
    response.on('data', (chunk) => {
      data += chunk;
    });
    response.on('end', () => {
      try {
        const decoded = distribution.util.deserialize(data);
        expect(typeof decoded).toBe("object");
        expect(decoded.length).toBe(2);
        expect(decoded[0]).toBe(null);
        expect(decoded[1]).toBe(null);
        distribution.local.routes.get("epsilon", (e, v) => {
          try {
            expect(e).toBeFalsy();
            expect(v).toBe("e");
            done();
          } catch (e) {
            done(e);
          }
        });
      } catch (e) {
        done(e);
      }
    });
  });

  request.on('error', (e) => {
    done(e);
  });

  request.write(distribution.util.serialize(["e", "epsilon"]));
  request.end();
});

test('comm throughput', (done) => {
  const iterations = 1000;
  const node = distribution.node.config;
  const remote = {node: node, service: 'status', method: 'get'};
  const message = ['nid'];
  const startTime = performance.now();
  let completed = 0;
  for (let i = 0; i < iterations; i++) {
    distribution.local.comm.send(message, remote, (e, v) => {
      completed++;
      if (completed == iterations) {
        const elapsed = performance.now() - startTime;
        console.log(`comm throughput: ${iterations / (elapsed / 1000)} ops/sec`);
        console.log(`time elapsed (s): ${elapsed / 1000}`);
        done();
      }
    });
  }
}, 100000);

test('comm latency', (done) => {
  const iterations = 1000;
  const node = distribution.node.config;
  const remote = {node: node, service: 'status', method: 'get'};
  const message = ['nid'];
  let totalTime = 0;
  let completed = 0;
  for (let i = 0; i < iterations; i++) {
    const startTime = performance.now();
    distribution.local.comm.send(message, remote, (e, v) => {
      const elapsed = performance.now() - startTime;
      completed++;
      totalTime += elapsed;
      if (completed == iterations) {
        console.log(`comm latency: ${totalTime / (1000 * 1000)} on average`);
        console.log(`time elapsed (s): ${totalTime / 1000}`);
        if (globalThis.distribution.node.server) {
          globalThis.distribution.node.server.close();
        }
        done();
      }
    });
  }
}, 100000);

// UNCOMMENT THESE TESTS AND COMMENT THE ABOVE TESTS TO TEST REFERENCE RPC THROUGHPUT/LATENCY
// test('rpc throughput', (done) => {
//   const iterations = 1000;
//   let n = 0;
//   const addOne = () => {
//     return ++n;
//   };

//   const node = {ip: '127.0.0.1', port: 9009};

//   let addOneRPC = globalThis.distribution.util.wire.createRPC(globalThis.distribution.util.wire.toAsync(addOne));

//   const rpcService = {
//     "addOne": addOneRPC,
//   };
//   distribution.node.start(() => {
//     function cleanup(callback) {
//       distribution.local.comm.send([],
//           {node: node, service: 'status', method: 'stop'},
//           callback);
//     }
//     distribution.local.status.spawn(node, (e, v) => {
//       distribution.local.comm.send([rpcService, 'addOneService'],
//         {node: node, service: 'routes', method: 'put'}, (e, v) => {
//           let completed = 0;
//           const startTime = performance.now();
//           for (let i = 0; i < iterations; i++) {
//           distribution.local.comm.send([],
//             {node: node, service: 'addOneService', method: 'addOne'}, (e, v) => {
//               completed++;
//               if (completed == iterations) {
//                 const elapsed = performance.now() - startTime;
//                 console.log(`rpc throughput: ${iterations / (elapsed / 1000)} ops/sec`);
//                 console.log(`time elapsed (s): ${elapsed / 1000}`);
//                 cleanup(done);
//               }
//             })}})});
//           });
// }, 100000);

// test('rpc latency', (done) => {
//   const iterations = 1000;
//   let n = 0;
//   const addOne = () => {
//     return ++n;
//   };

//   const node = {ip: '127.0.0.1', port: 9010};

//   let addOneRPC = globalThis.distribution.util.wire.createRPC(globalThis.distribution.util.wire.toAsync(addOne));

//   const rpcService = {
//     "addOne": addOneRPC,
//   };
//   function cleanup(callback) {
//     globalThis.distribution.node.server.close();
//     distribution.local.comm.send([],
//         {node: node, service: 'status', method: 'stop'},
//         callback);
//   }
//   distribution.local.status.spawn(node, (e, v) => {
//     distribution.local.comm.send([rpcService, 'addOneService'],
//       {node: node, service: 'routes', method: 'put'}, (e, v) => {
//         let completed = 0;
//         let totalTime = 0;
//         for (let i = 0; i < iterations; i++) {
//           const startTime = performance.now();
//           distribution.local.comm.send([],
//             {node: node, service: 'addOneService', method: 'addOne'}, (e, v) => {
//               const elapsed = performance.now() - startTime;
//               totalTime += elapsed;
//               completed++;
//               if (completed == iterations) {
//                 console.log(`rpc latency: ${totalTime / (1000 * 1000)} on average`);
//                 console.log(`time elapsed (s): ${totalTime / 1000}`);
//                 cleanup(done);
//               }
//           })}})});
// }, 100000);
