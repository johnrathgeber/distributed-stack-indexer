/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Imporant: Do not modify any of the test headers (i.e., the test('header', ...) part). Doing so will result in grading penalties.
*/

const distribution = require('../../distribution.js')();
require('../helpers/sync-guard');

test('(1 pts) student test', (done) => {
  // Fill out this test case...
  distribution.node.start(() => {
    function cleanup(callback) {
      // if (globalThis.distribution.node.server) {
      //   globalThis.distribution.node.server.close();
      // }
      callback();
    }
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
              cleanup(done);
            }
            catch (e) {
              cleanup(() => {done(e);});
            }
          });
        }
        catch (e) {
          cleanup(() => {done(e);});
        }
      });
    }
    catch (e) {
      cleanup(() => {done(e);});
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
  distribution.local.routes.put("a", "aardvark", (e, v) => {
    try {
      expect(e).toBeFalsy();
      expect(v).toBe(null);
      distribution.local.routes.put("b", "balloon", (e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v).toBe(null);
          distribution.local.routes.rem("a", (e, v) => {
            try {
              expect(e).toBeDefined();
              expect(e).toBeInstanceOf(Error);
              expect(v).toBeFalsy();
              distribution.local.routes.rem("balloon", (e, v) => {
                try {
                  expect(e).toBeFalsy();
                  expect(v).toBe("b");
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
  // Fill out this test case...
  // distribution.node.start(() => {
    function cleanup(callback) {
      if (globalThis.distribution.node.server) {
        globalThis.distribution.node.server.close();
      }
      callback();
    }
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
          expect(decoded[0]).toBe(null);
          expect(decoded[1]).toBe(null);
          distribution.local.routes.get("aardvark", (e, v) => {
            try {
              expect(e).toBeFalsy();
              expect(v).toBe("a");
              cleanup(done);
            } catch (e) {
              cleanup(() => {done(e);});
            }
          });
        } catch (e) {
          cleanup(() => {done(e);});
        }
      });
    });

    request.on('error', (e) => {
      cleanup(() => {done(e);});
    });

    request.write(distribution.util.serialize(["a", "aardvark"]));
    request.end();
  // });
});
