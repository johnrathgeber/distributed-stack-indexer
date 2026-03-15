// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Node} Node
 */

/**
 * Map functions used for mapreduce
 * @callback Mapper
 * @param {string} key
 * @param {any} value
 * @returns {object[]}
 */

/**
 * Reduce functions used for mapreduce
 * @callback Reducer
 * @param {string} key
 * @param {any[]} value
 * @returns {object}
 */

/**
 * @typedef {Object} MRConfig
 * @property {Mapper} map
 * @property {Reducer} reduce
 * @property {string[]} keys
 *
 */

const util = require("../util/util.js");
const fs = require('fs');

let mrConfig = null;
let mrServName = null;
let mrGid = null;

/**
 * @param {MRConfig} configuration
 * @param {Node} coordNode
 * @param {string} servName
 * @param {Callback} callback
 */
function recvMapReduce(configuration, coordNode, servName, gid, callback) {
    mrConfig = configuration;
    mrServName = servName;
    mrGid = gid;
    const coordinatorRemote = {node: coordNode, service: servName, method: "notify"};
    globalThis.distribution.local.comm.send([], coordinatorRemote, (e, v) => {
        if (e) {
            callback(e);
            return;
        }
        callback(null, {});
    });
};

/**
 * @param {Callback} callback
 */
function mapIt(nodeNIDs, sidToNode, callback) {
    const mySID = util.id.getSID(globalThis.distribution.node.config);
    const myNID = util.id.getNID(sidToNode[mySID]);
    const myKeys = mrConfig.keys.filter(key => util.id.consistentHash(util.id.getID(key), nodeNIDs) === myNID);
    const total = myKeys.length;
    let cntr = 0;
    if (total == 0) {
        callback(null, {});
        return;
    }
    for (const key of myKeys) {
        globalThis.distribution.local.store.get({key: key, gid: mrGid}, (e, v) => {
            if (e) {
                cntr++;
                if (cntr == total) {
                    callback(null, {});
                }
                return;
            }
            const mapped = mrConfig.map(key, v);
            globalThis.distribution.local.store.put(mapped, {key: key, gid: mrServName + "map"}, (e, v) => {
                if (e) {
                    callback(e);
                    return;
                }
                cntr++;
                if (cntr == total) {
                    callback(null, {});
                }
            });
        });
    }
};

/**
 * @param {Callback} callback
 */
function shuffleIt(nodeNIDs, sidToNode, callback) {
    const mySID = util.id.getSID(globalThis.distribution.node.config);
      const myNID = util.id.getNID(sidToNode[mySID]);
      const myKeys = mrConfig.keys.filter(key => util.id.consistentHash(util.id.getID(key), nodeNIDs) === myNID);
      const total = myKeys.length;
      let cntr = 0;
      let newKeys = [];
      if (total == 0) {
          callback(null, newKeys);
          return;
      }
      for (const key of myKeys) {
        globalThis.distribution.local.store.get({key: key, gid: mrServName + "map"}, (e, v) => {
            if (e) {
                cntr++;
                if (cntr == total) {
                    // fs.appendFileSync('/tmp/mr-debug.log', JSON.stringify({newKeys}) + '\n');
                    callback(null, newKeys);
                }
                return;
            }
            let cntr2 = 0;
            v = Array.isArray(v) ? v : [v];
            const total2 = v.length;
            if (total2 == 0) {
                cntr++;
                if (cntr == total) {
                    // fs.appendFileSync('/tmp/mr-debug.log', JSON.stringify({newKeys}) + '\n');
                    callback(null, newKeys);
                }
                return;
            }
            for (const obj of v) {
                const shuffleKey = Object.keys(obj)[0];
                newKeys.push(shuffleKey);
                const nid = util.id.consistentHash(util.id.getID(shuffleKey), nodeNIDs);
                const sid = nid.substring(0, 5);
                const node = sidToNode[sid];
                const remote = {node: node, service: "store", method: "append"};
                globalThis.distribution.local.comm.send([obj[shuffleKey], {key: shuffleKey, gid: mrServName}], remote, (e, v) => {
                    if (e) {
                        callback(e);
                        return;
                    }
                    cntr2++;
                    if (cntr2 == total2) {
                        cntr++;
                        if (cntr == total) {
                            // fs.appendFileSync('/tmp/mr-debug.log', JSON.stringify({newKeys}) + '\n');
                            callback(null, newKeys);
                        }
                    }
                });
            }
        });
    }
};

/**
 * @param {Callback} callback
 */
function reduceIt(keys, nodeNIDs, sidToNode, callback) {
    const mySID = util.id.getSID(globalThis.distribution.node.config);
    const myNID = util.id.getNID(sidToNode[mySID]);
    const myKeys = keys.filter(key => util.id.consistentHash(util.id.getID(key), nodeNIDs) === myNID);
    // fs.appendFileSync('/tmp/mr-debug.log', JSON.stringify({myKeys}) + '\n');
    const total = myKeys.length;
    let cntr = 0;
    let toRtn = [];
    if (total == 0) {
        // fs.appendFileSync('/tmp/mr-debug.log', JSON.stringify({toRtn}) + '\n');
        callback(null, toRtn);
        return;
    }
    for (const key of myKeys) {
        globalThis.distribution.local.store.get({key: key, gid: mrServName}, (e, v) => {
            if (e) {
                cntr++;
                if (cntr == total) {
                    // fs.appendFileSync('/tmp/mr-debug.log', JSON.stringify({toRtn}) + '\n');
                    callback(null, toRtn);
                }
                return;
            }
            const reduced = mrConfig.reduce(key, v);
            cntr++;
            toRtn.push(reduced);
            if (cntr == total) {
                // fs.appendFileSync('/tmp/mr-debug.log', JSON.stringify({toRtn}) + '\n');
                callback(null, toRtn);
            }
        });
    }
};

module.exports = {recvMapReduce, mapIt, shuffleIt, reduceIt};
