/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Imporant: Do not modify any of the test headers (i.e., the test('header', ...) part). Doing so will result in grading penalties.
*/

const distribution = require('../../distribution.js')();
const id = require('../../distribution/util/id.js');
require('../helpers/sync-guard');

test('(1 pts) student test', (done) => {
  const user = {first: 'Josiah', last: 'Carberry'};
  const key = 'jcarbmp';

  distribution.local.mem.put(user, key, (e, v) => {
    try {
      expect(e).toBeFalsy();
      expect(v).toEqual(user);
      distribution.local.mem.put(user, key, (e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v).toEqual(user);
          distribution.local.mem.put(user, key, (e, v) => {
            try {
              expect(e).toBeFalsy();
              expect(v).toEqual(user);
              done();
            } catch (error) {
              done(error);
            }
          });
        } catch (error) {
          done(error);
        }
      });
    } catch (error) {
      done(error);
    }
  });
});


test('(1 pts) student test', (done) => {
  const user = {first: 'Josiah', last: 'Carberry'};
  const key = 'jcarbsp';

  distribution.local.store.put(user, key, (e, v) => {
    try {
      expect(e).toBeFalsy();
      expect(v).toEqual(user);
      distribution.local.store.put(user, key, (e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v).toEqual(user);
          distribution.local.store.put(user, key, (e, v) => {
            try {
              expect(e).toBeFalsy();
              expect(v).toEqual(user);
              done();
            } catch (error) {
              done(error);
            }
          });
        } catch (error) {
          done(error);
        }
      });
    } catch (error) {
      done(error);
    }
  });
});


test('(1 pts) student test', (done) => {
  const key = 'samekey';
  const dkey = 'diffkey';
  const a = {a: 1};
  const b = {b: 2};
  const c = {c: 3};

  distribution.mygroup.mem.put(a, key, (e, v) => {
    if (e) return done(e);
    distribution.mygroup.mem.put(b, {key, gid: 'group1'}, (e, v) => {
      if (e) return done(e);
      distribution.mygroup.mem.get(key, (e, v) => {
        if (e) return done(e);
        expect(v).toEqual(a);
        distribution.mygroup.mem.put(c, dkey, (e, v) => {
          if (e) return done(e);
          distribution.mygroup.mem.put(c, {key: dkey, gid: 'group1'}, (e, v) => {
            if (e) return done(e);
            distribution.mygroup.mem.del(dkey, (e, v) => {
              if (e) return done(e);
              expect(v).toEqual(c);
              distribution.mygroup.mem.get(dkey, (e, v) => {
                expect(e).toBeDefined();
                done();
              });
            });
          });
        });
      });
    });
  });
});

test('(1 pts) student test', (done) => {
  const user = {first: 'Josiah', last: 'Carberry'};
  const key = 'jcarbsp';

  distribution.local.store.put(user, key, (e, v) => {
    try {
      expect(e).toBeFalsy();
      expect(v).toEqual(user);
      distribution.local.store.del(key, (e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v).toEqual(user);
          distribution.local.store.del(key, (e, v) => {
            try {
              expect(e).toBeDefined();
              done();
            } catch (error) {
              done(error);
            }
          });
        } catch (error) {
          done(error);
        }
      });
    } catch (error) {
      done(error);
    }
  });
});

test('(1 pts) student test', (done) => {
  const key = 'key';
  const userA = {first: 'Alice', group: 'A'};
  const userB = {first: 'Bob', group: 'B'};
  const key2 = "keykey";

  distribution.mygroup.store.put(userA, key, (e) => {
    if (e) return done(e);
    distribution.group2.store.put(userB, key, (e) => {
      if (e) return done(e);
      distribution.mygroup.store.get(key, (eA, vA) => {
        try {
          expect(eA).toBeFalsy();
          expect(vA).toEqual(userA);
          distribution.group2.store.get(key, (eB, vB) => {
            try {
              expect(eB).toBeFalsy();
              expect(vB).toEqual(userB);
              distribution.group2.store.put(userA, key2, (eB, vB) => {
                try {
                  expect(eB).toBeFalsy();
                  expect(vB).toEqual(userA);
                  distribution.group2.store.get(key2, (eB, vB) => {
                    try {
                      expect(eB).toBeFalsy();
                      expect(vB).toEqual(userA);
                      distribution.mygroup.store.get(key2, (eA, vA) => {
                        try {
                          expect(eA).toBeDefined();
                          distribution.mygroup.store.get(key2, (eA, vA) => {
                            try {
                              expect(eA).toBeDefined();
                              done();
                            } catch (err) {
                              done(err);
                            }
                          });
                        } catch (err) {
                          done(err);
                        }
                      });
                    } catch (err) {
                      done(err);
                    }
                  });
                } catch (err) {
                  done(err);
                }
              });
            } catch (err) {
              done(err);
            }
          });
        } catch (err) {
          return done(err);
        }
      });
    });
  });
});

const mygroupGroup = {};
const group2Group = {};

const n1 = {ip: '127.0.0.1', port: 8000};
const n2 = {ip: '127.0.0.1', port: 8001};
beforeAll((done) => {
  // First, stop the nodes if they are running
  const remote = {service: 'status', method: 'stop'};

  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {});
  });

  mygroupGroup[id.getSID(n1)] = n1;

  group2Group[id.getSID(n2)] = n2;

  distribution.node.start((e) => {
    if (e) {
      done(e);
      return;
    }
    const groupInstantiation = (e, v) => {
      const mygroupConfig = {gid: 'mygroup'};
      const group2Config = {gid: 'group2', hash: id.consistentHash};
      // Create some groups
      distribution.local.groups
          .put(mygroupConfig, mygroupGroup, (e, v) => {
            distribution.local.groups
              .put(group2Config, group2Group, (e, v) => {
                  done();
                });
              });
    };

    // Start the nodes
    distribution.local.status.spawn(n1, (e, v) => {
      if (e) {
        done(e);
        return;
      }
      distribution.local.status.spawn(n2, (e, v) => {
        if (e) {
          done(e);
          return;
        }
        groupInstantiation();
      });
    });
  });
});

afterAll((done) => {
  distribution.mygroup.status.stop((e, v) => {
    const remote = {service: 'status', method: 'stop'};
    remote.node = n1;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n2;
      if (globalThis.distribution.node.server) {
        globalThis.distribution.node.server.close();
      }
      done();
    });
  });
});

