// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 * @typedef {import("../types.js").Hasher} Hasher
 * @typedef {import("../types.js").Node} Node
 */

const { id } = require("../util/util.js");

/**
 * @typedef {Object} StoreConfig
 * @property {string | null} key
 * @property {string} gid
 *
 * @typedef {StoreConfig | string | null} SimpleConfig
 */


/**
 * @param {Config} config
 */
function store(config) {
  const context = {
    gid: config.gid || 'all',
    hash: config.hash || globalThis.distribution.util.id.consistentHash,
    subset: config.subset,
  };

  /**
   * @param {SimpleConfig} configuration
   * @param {Callback} callback
   */
  function get(configuration, callback) {
    if (configuration === null) {
      globalThis.distribution.local.groups.get(context.gid, (e, v) => {
        if (e) { callback(e); return; }
        const nodes = Object.values(v);
        let allKeys = [];
        let cntr = 0;
        for (const node of nodes) {
          const remote = {node: node, service: 'store', method: 'get'};
          globalThis.distribution.local.comm.send([{gid: context.gid, key: null}], remote, (e, v) => {
            if (!e) {
              allKeys = allKeys.concat(v);
            }
            cntr++;
            if (cntr === nodes.length) {
              callback(null, [...new Set(allKeys)]);
            }
          });
        }
      });
      return;
    }
    const gid = typeof configuration == "string" ? context.gid : configuration.gid;
    globalThis.distribution.local.groups.get(gid, (e, v) => {
      if (e) {
        callback(e);
        return;
      }
      const kid = typeof configuration == "string" ? configuration : configuration.key;
      const nids = Object.values(v).map(x => id.getNID(x));
      const nid = context.hash(id.getID(kid), nids);
      let target = null;
      for (const [k, v2] of Object.entries(v)) {
        if (id.getNID(v2) == nid) {
          target = v2;
        }
      }
      if (!target) {
        callback(new Error(`Error while resolving to hashed node.`));
        return;
      }
      const remote = {node: target, service: "store", method: "get"};
      globalThis.distribution.local.comm.send([{gid: gid, key: kid}], remote, (e, v) => {
        if (e) {
          callback(new Error(`Error while doing distributed store get: ${e.message}`));
        }
        else {
          callback(null, v);
        }
      });
    });
  }

  /**
   * @param {any} state
   * @param {SimpleConfig} configuration
   * @param {Callback} callback
   */
  function put(state, configuration, callback) {
    const gid = typeof configuration == "string" || configuration === null ? context.gid : configuration.gid;
    globalThis.distribution.local.groups.get(gid, (e, v) => {
      if (e) {
        callback(e);
        return;
      }
      const kid = configuration === null ? id.getID(state)
        : typeof configuration == "string" ? configuration
        : configuration.key;
      const nids = Object.values(v).map(x => id.getNID(x));
      const nid = context.hash(id.getID(kid), nids);
      let target = null;
      for (const [k, v2] of Object.entries(v)) {
        if (id.getNID(v2) == nid) {
          target = v2;
        }
      }
      if (!target) {
        callback(new Error(`Error while resolving to hashed node.`));
        return;
      }
      const remote = {node: target, service: "store", method: "put"};
      globalThis.distribution.local.comm.send([state, {gid: gid, key: kid}], remote, (e, v) => {
        if (e) {
          callback(new Error(`Error while doing distributed store put: ${e.message}`));
        }
        else {
          callback(null, v);
        }
      });
    });
  }

  /**
   * @param {any} state
   * @param {SimpleConfig} configuration
   * @param {Callback} callback
   */
  function append(state, configuration, callback) {
    return callback(new Error('store.append not implemented'));
  }

  /**
   * @param {SimpleConfig} configuration
   * @param {Callback} callback
   */
  function del(configuration, callback) {
    const gid = typeof configuration == "string" ? context.gid : configuration.gid;
    globalThis.distribution.local.groups.get(gid, (e, v) => {
      if (e) {
        callback(e);
        return;
      }
      const kid = typeof configuration == "string" ? configuration
        : configuration.key;
      const nids = Object.values(v).map(x => id.getNID(x));
      const nid = context.hash(id.getID(kid), nids);
      let target = null;
      for (const [k, v2] of Object.entries(v)) {
        if (id.getNID(v2) == nid) {
          target = v2;
        }
      }
      if (!target) {
        callback(new Error(`Error while resolving to hashed node.`));
        return;
      }
      const remote = {node: target, service: "store", method: "del"};
      globalThis.distribution.local.comm.send([{gid: gid, key: kid}], remote, (e, v) => {
        if (e) {
          callback(new Error(`Error while doing distributed store del: ${e.message}`));
        }
        else {
          callback(null, v);
        }
      });
    });
  }

  /**
   * @param {Object.<string, Node>} configuration
   * @param {Callback} callback
   */
  function reconf(configuration, callback) {
    return callback(new Error('store.reconf not implemented'));
  }

  /* For the distributed store service, the configuration will
          always be a string */
  return {get, put, append, del, reconf};
}

module.exports = store;
