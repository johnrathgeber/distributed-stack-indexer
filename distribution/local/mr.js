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

    const CHUNK = 100;
    let keyIdx = 0;

    function mapChunk() {
        const chunk = myKeys.slice(keyIdx, keyIdx + CHUNK);
        keyIdx += CHUNK;
        let remaining = chunk.length;

        for (const key of chunk) {
            globalThis.distribution.local.store.get({key: key, gid: mrGid}, (e, v) => {
                if (e) {
                    remaining--;
                    if (remaining == 0) {
                        if (keyIdx >= total) {
                            callback(null, {});
                        } else {
                            mapChunk();
                        }
                    }
                    return;
                }
                const mapped = mrConfig.map(key, v);
                globalThis.distribution.local.store.put(mapped, {key: key, gid: mrServName + "map"}, (e, v) => {
                    if (e) {
                        callback(e);
                        return;
                    }
                    remaining--;
                    if (remaining == 0) {
                        if (keyIdx >= total) {
                            callback(null, {});
                        } else {
                            mapChunk();
                        }
                    }
                });
            });
        }
    }

    mapChunk();
};

/**
 * @param {Callback} callback
 */
function shuffleIt(nodeNIDs, sidToNode, callback) {
    const mySID = globalThis.distribution.util.id.getSID(globalThis.distribution.node.config);
    const myNID = globalThis.distribution.util.id.getNID(sidToNode[mySID]);
    const myKeys = mrConfig.keys.filter(key => globalThis.distribution.util.id.consistentHash(globalThis.distribution.util.id.getID(key), nodeNIDs) === myNID);
    const total = myKeys.length;
    const newKeysSet = new Set();
    if (total == 0) {
        callback(null, []);
        return;
    }

    const CHUNK = 500;
    let keyIdx = 0;

    function processChunk() {
        const chunk = myKeys.slice(keyIdx, keyIdx + CHUNK);
        keyIdx += CHUNK;
        let remaining = chunk.length;
        const nodeBatches = {};

        for (const key of chunk) {
            globalThis.distribution.local.store.get({key: key, gid: mrServName + "map"}, (e, v) => {
                if (!e && v) {
                    v = Array.isArray(v) ? v : [v];
                    for (const obj of v) {
                        const shuffleKey = Object.keys(obj)[0];
                        newKeysSet.add(shuffleKey);
                        const nid = globalThis.distribution.util.id.consistentHash(
                            globalThis.distribution.util.id.getID(shuffleKey), nodeNIDs
                        );
                        const sid = nid.substring(0, 5);
                        if (!nodeBatches[sid]) {
                            nodeBatches[sid] = [];
                        }
                        nodeBatches[sid].push({key: shuffleKey, value: obj[shuffleKey]});
                    }
                }
                remaining--;
                if (remaining === 0) {
                    const sids = Object.keys(nodeBatches);
                    if (sids.length === 0) {
                        if (keyIdx >= total) {
                            callback(null, [...newKeysSet]);
                        } else {
                            processChunk();
                        }
                        return;
                    }
                    let batchCntr = 0;
                    for (const sid of sids) {
                        const node = sidToNode[sid];
                        const remote = {node, service: 'store', method: 'batchAppend'};
                        globalThis.distribution.local.comm.send(
                            [nodeBatches[sid], {gid: mrServName}],
                            remote,
                            (e) => {
                                if (e) {
                                    callback(e);
                                    return;
                                }
                                batchCntr++;
                                if (batchCntr === sids.length) {
                                    if (keyIdx >= total) {
                                        callback(null, [...newKeysSet]);
                                    } else {
                                        processChunk();
                                    }
                                }
                            }
                        );
                    }
                }
            });
        }
    }

    processChunk();
};

/**
 * @param {Callback} callback
 */
function reduceIt(keys, nodeNIDs, sidToNode, callback) {
    const mySID = globalThis.distribution.util.id.getSID(globalThis.distribution.node.config);
    const myNID = globalThis.distribution.util.id.getNID(sidToNode[mySID]);
    const myKeys = keys.filter(key => globalThis.distribution.util.id.consistentHash(globalThis.distribution.util.id.getID(key), nodeNIDs) === myNID);
    const total = myKeys.length;
    if (total == 0) {
        callback(null, []);
        return;
    }

    const CHUNK = 100;
    let keyIdx = 0;

    function reduceChunk() {
        const chunk = myKeys.slice(keyIdx, keyIdx + CHUNK);
        keyIdx += CHUNK;
        let remaining = chunk.length;

        for (const key of chunk) {
            globalThis.distribution.local.store.get({key: key, gid: mrServName}, (e, v) => {
                if (e) {
                    remaining--;
                    if (remaining == 0) {
                        if (keyIdx >= total) {
                            callback(null, []);
                        } else {
                            reduceChunk();
                        }
                    }
                    return;
                }
                const reduced = mrConfig.reduce(key, v);
                const term = Object.keys(reduced)[0];
                globalThis.distribution.index.store.put(reduced[term], globalThis.distribution.util.id.getID(term), (e) => {
                    remaining--;
                    if (remaining == 0) {
                        if (keyIdx >= total) {
                            callback(null, []);
                        } else {
                            reduceChunk();
                        }
                    }
                });
            });
        }
    }

    reduceChunk();
};

module.exports = {recvMapReduce, mapIt, shuffleIt, reduceIt};
