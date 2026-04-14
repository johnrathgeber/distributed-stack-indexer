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
    const mySID = globalThis.distribution.util.id.getSID(globalThis.distribution.node.config);
    const myNID = globalThis.distribution.util.id.getNID(sidToNode[mySID]);
    const myKeys = mrConfig.keys.filter(key => globalThis.distribution.util.id.consistentHash(globalThis.distribution.util.id.getID(key), nodeNIDs) === myNID);
    const total = myKeys.length;
    if (total == 0) {
        callback(null, {});
        return;
    }
    // Limit concurrent file I/O to avoid exhausting file descriptors.
    const CONCURRENCY = 100;
    let next = 0;
    let done = 0;
    let inflight = 0;
    let failed = false;

    function dispatch() {
        while (!failed && inflight < CONCURRENCY && next < total) {
            const key = myKeys[next++];
            inflight++;
            globalThis.distribution.local.store.get({key: key, gid: mrGid}, (e, v) => {
                if (failed) return;
                if (e) {
                    inflight--;
                    done++;
                    if (done === total) callback(null, {});
                    else dispatch();
                    return;
                }
                const mapped = mrConfig.map(key, v);
                globalThis.distribution.local.store.put(mapped, {key: key, gid: mrServName + 'map'}, (e2) => {
                    if (failed) return;
                    inflight--;
                    if (e2) { failed = true; callback(e2); return; }
                    done++;
                    if (done === total) callback(null, {});
                    else dispatch();
                });
            });
        }
    }

    dispatch();
};

/**
 * @param {Callback} callback
 */
function shuffleIt(nodeNIDs, sidToNode, callback) {
    const mySID = globalThis.distribution.util.id.getSID(globalThis.distribution.node.config);
    const myNID = globalThis.distribution.util.id.getNID(sidToNode[mySID]);
    const myKeys = mrConfig.keys.filter(key => globalThis.distribution.util.id.consistentHash(globalThis.distribution.util.id.getID(key), nodeNIDs) === myNID);
    const total = myKeys.length;
    const newKeys = [];
    if (total == 0) {
        callback(null, newKeys);
        return;
    }

    // Phase 1: read all mapped outputs synchronously (store.get uses readFileSync).
    // Phase 2: send all shuffle items with limited concurrency.
    const CONCURRENCY = 100;
    const pendingSends = [];

    // store.get fires its callback synchronously (readFileSync), so a flat loop
    // is correct and avoids the deep recursion that a dispatch-style approach
    // would create (which overflows the call stack for large key sets).
    for (const key of myKeys) {
        globalThis.distribution.local.store.get({key: key, gid: mrServName + 'map'}, (e, v) => {
            if (!e) {
                v = Array.isArray(v) ? v : [v];
                for (const obj of v) {
                    const shuffleKey = Object.keys(obj)[0];
                    newKeys.push(shuffleKey);
                    const nid = globalThis.distribution.util.id.consistentHash(
                        globalThis.distribution.util.id.getID(shuffleKey), nodeNIDs);
                    const sid = nid.substring(0, 5);
                    pendingSends.push({value: obj[shuffleKey], key: shuffleKey, node: sidToNode[sid]});
                }
            }
        });
    }

    function startSends() {
        if (pendingSends.length === 0) {
            callback(null, newKeys);
            return;
        }
        let sendNext = 0;
        let sendDone = 0;
        let sendInflight = 0;
        let sendFailed = false;
        const totalSends = pendingSends.length;

        function dispatchSends() {
            while (!sendFailed && sendInflight < CONCURRENCY && sendNext < totalSends) {
                const {value, key: sk, node} = pendingSends[sendNext++];
                sendInflight++;
                const remote = {node, service: 'store', method: 'append'};
                globalThis.distribution.local.comm.send([value, {key: sk, gid: mrServName}], remote, (e) => {
                    if (sendFailed) return;
                    sendInflight--;
                    if (e) { sendFailed = true; callback(e); return; }
                    sendDone++;
                    if (sendDone === totalSends) {
                        callback(null, newKeys);
                    } else {
                        dispatchSends();
                    }
                });
            }
        }

        dispatchSends();
    }

    startSends();
};

/**
 * @param {Callback} callback
 */
function reduceIt(keys, nodeNIDs, sidToNode, callback) {
    const mySID = globalThis.distribution.util.id.getSID(globalThis.distribution.node.config);
    const myNID = globalThis.distribution.util.id.getNID(sidToNode[mySID]);
    const myKeys = keys.filter(key => globalThis.distribution.util.id.consistentHash(globalThis.distribution.util.id.getID(key), nodeNIDs) === myNID);
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
