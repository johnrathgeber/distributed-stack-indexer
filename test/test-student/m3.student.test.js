/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Imporant: Do not modify any of the test headers (i.e., the test('header', ...) part). Doing so will result in grading penalties.
*/

const distribution = require('../../distribution.js')();
require('../helpers/sync-guard');

const id = distribution.util.id;

const n1 = {ip: '127.0.0.1', port: 7000};
const n2 = {ip: '127.0.0.1', port: 7001};

const n1SID = id.getSID(n1);
const n2SID = id.getSID(n2);

test('(1 pts) student test', (done) => {
  const groupC = {};
  groupC[n2SID] = n2;
  groupC[n1SID] = n1;

  const config = {gid: 'groupC'};

  distribution.local.groups.put(config, groupC, (e, v) => {
    distribution.groupC.groups.put(config, groupC, (e, v) => {
      const remote = {node: n1, service: "groups", method: "rem"};
      distribution.local.comm.send(["groupC", n1SID], remote, (e, v) => {
        distribution.local.comm.send(["groupC", n2SID], remote, (e, v) => {
          distribution.groupC.groups.get('groupC', (e, v) => {
            const n1View = v[n1SID];
            const n2View = v[n2SID];
            try {
              expect(Object.keys(n2View)).toEqual(expect.arrayContaining(
                  [n1SID, n2SID],
              ));
              expect(Object.keys(n1View)).toEqual([]);
              done();
            } catch (error) {
              done(error);
            }
          });
        });
      });
    });
  });
});


test('(1 pts) student test', (done) => {
  const groupC = {};
  groupC[n2SID] = n2;
  groupC[n1SID] = n1;

  const config = {gid: 'groupC'};

  distribution.local.groups.put(config, groupC, (e, v) => {
    distribution.groupC.groups.put(config, groupC, (e, v) => {
      const remote = {service: "groups", method: "rem"};
      distribution.groupC.comm.send(["groupC", n1SID], remote, (e, v) => {
        distribution.groupC.comm.send(["groupC", n2SID], remote, (e, v) => {
          distribution.groupC.groups.get('groupC', (e, v) => {
            try {
              expect(e).toEqual({});
              expect(v[n1SID]).toEqual({});
              expect(v[n2SID]).toEqual({});
              done();
            }
            catch (error) {
              done(error);
            }
          });
        });
      });
    });
  });
});


test('(1 pts) student test', (done) => {
  const groupC = {};
  groupC[n2SID] = n2;
  groupC[n1SID] = n1;

  const config = {gid: 'groupC'};

  distribution.local.groups.put(config, groupC, (e, v) => {
    distribution.groupC.groups.put(config, groupC, (e, v) => {
      const remote = {service: "groups", method: "rem"};
      distribution.groupC.comm.send(["groupC", n1SID], remote, (e, v) => {
        distribution.groupC.status.get("nid", (e, v) => {
          try {
            expect(e).toEqual({});
            expect(Object.values(v)).toEqual(expect.arrayContaining([id.getNID(n1), id.getNID(n2)]));
            done();
          }
          catch (error) {
            done(error);
          }
        });
      });
    });
  });
});

test('(1 pts) student test', (done) => {
  const groupC = {};
  groupC[n2SID] = n2;
  groupC[n1SID] = n1;

  const config = {gid: 'groupC'};

  distribution.local.groups.put(config, groupC, (e, v) => {
    distribution.groupC.groups.put(config, groupC, (e, v) => {
      const remote = {node: n1, service: "groups", method: "rem"};
      distribution.local.comm.send(["groupC", n1SID], remote, (e, v) => {
        distribution.all.groups.get("groupC", (e, v) => {
          try {
            expect(e).toEqual({});
            expect(Object.keys(v)).toEqual(expect.arrayContaining([n1SID, n2SID]));
            expect(Object.keys(v[n1SID])).toEqual([n2SID]);
            expect(v[n1SID][n2SID].port).toEqual(n2.port);
            expect(Object.keys(v[n2SID])).toEqual(expect.arrayContaining([n1SID, n2SID]));
            expect(v[n2SID][n1SID].port).toEqual(n1.port);
            expect(v[n2SID][n2SID].port).toEqual(n2.port);
            done();
          }
          catch (error) {
            done(error);
          }
        });
      });
    });
  });
});

test('(1 pts) student test', (done) => {
  const groupC = {};
  groupC[n2SID] = n2;
  groupC[n1SID] = n1;

  const config = {gid: 'groupC'};

  distribution.local.groups.put(config, groupC, (e, v) => {
    distribution.groupC.groups.put(config, groupC, (e, v) => {
      distribution.all.groups.del("groupC", (e, v) => {
        try {
          expect(e).toEqual({});
          expect(Object.keys(v)).toEqual(expect.arrayContaining([n1SID, n2SID]));
          expect(Object.keys(v[n1SID])).toEqual(expect.arrayContaining([n1SID, n2SID]));
          expect(v[n1SID][n1SID].port).toEqual(n1.port);
          expect(v[n1SID][n2SID].port).toEqual(n2.port);
          expect(Object.keys(v[n2SID])).toEqual(expect.arrayContaining([n1SID, n2SID]));
          expect(v[n2SID][n1SID].port).toEqual(n1.port);
          expect(v[n2SID][n2SID].port).toEqual(n2.port);
          done();
        }
        catch (error) {
          done(error);
        }
      });
    });
  });
});

beforeAll((done) => {
    distribution.node.start((e) => {
      if (e) return done(e);
      distribution.local.status.spawn(n1, (e, v) => {
        if (e) return done(e);
        distribution.local.status.spawn(n2, (e, v) => {
          if (e) return done(e);
          done();
        });
      });
    });
  });

afterAll((done) => {
  const remote = {service: 'status', method: 'stop'};
  distribution.local.comm.send([], {...remote, node: n1}, (e, v) => {
    distribution.local.comm.send([], {...remote, node: n2}, (e, v) => {
      if (globalThis.distribution.node.server) {
        globalThis.distribution.node.server.close();
      }
      done();
    });
  });
});
