// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 * @typedef {import("../types.js").Node} Node
 */

const { id } = require("../util/util.js");


/**
 * @typedef {Object} StoreConfig
 * @property {string | null} key
 * @property {string} gid
 *
 * @typedef {StoreConfig | string | null} SimpleConfig
 *
 * @typedef {Object} Mem
 * @property {(configuration: SimpleConfig, callback: Callback) => void} get
 * @property {(state: any, configuration: SimpleConfig, callback: Callback) => void} put
 * @property {(state: any, configuration: SimpleConfig, callback: Callback) => void} append
 * @property {(configuration: SimpleConfig, callback: Callback) => void} del
 * @property {(configuration: Object.<string, Node>, callback: Callback) => void} reconf
 */


/**
 * @param {Config} config
 * @returns {Mem}
 */
function mem(config) {
  const context = {};
  context.gid = config.gid || 'all';
  context.hash = config.hash || globalThis.distribution.util.id.naiveHash;

  /**
   * @param {SimpleConfig} configuration
   * @param {Callback} callback
   */
  function get(configuration, callback) {
    const gid = typeof configuration == "string" ? context.gid : configuration.gid;
    globalThis.distribution.local.groups.get(gid, (e, v) => {
      if (e) {
        callback(e);
        return;
      }
      const kid = typeof configuration == "string" ? configuration : configuration.key;
      const nids = Object.values(v).map(x => id.getNID(x));
      const nid = context.hash(id.getID(kid), nids);
      const target = Object.values(v).find(n => id.getNID(n) == nid);
      const remote = {node: target, service: "mem", method: "get"};
      globalThis.distribution.local.comm.send([{gid: gid, key: kid}], remote, (e, v) => {
        if (e) {
          callback(new Error(`Error while doing distributed mem get: ${e.message}`));
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
    globalThis.distribution.local.groups.get(context.gid, (e, v) => {
      if (e) {
        callback(e);
        return;
      }
      const kid = configuration === null ? id.getID(state)
        : typeof configuration == "string" ? configuration
        : configuration.key;
      const nids = Object.values(v).map(x => id.getNID(x));
      const nid = context.hash(id.getID(kid), nids);
      const target = Object.values(v).find(n => id.getNID(n) == nid);
      const remote = {node: target, service: "mem", method: "put"};
      globalThis.distribution.local.comm.send([state, {gid: gid, key: kid}], remote, (e, v) => {
        if (e) {
          callback(new Error(`Error while doing distributed mem put: ${e.message}`));
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
    return callback(new Error('mem.append not implemented'));
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
      const target = Object.values(v).find(n => id.getNID(n) == nid);
      const remote = {node: target, service: "mem", method: "del"};
      globalThis.distribution.local.comm.send([{gid: gid, key: kid}], remote, (e, v) => {
        if (e) {
          callback(new Error(`Error while doing distributed mem del: ${e.message}`));
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
    return callback(new Error('mem.reconf not implemented'));
  }
  /* For the distributed mem service, the configuration will
          always be a string */
  return {
    get,
    put,
    append,
    del,
    reconf,
  };
}

module.exports = mem;
