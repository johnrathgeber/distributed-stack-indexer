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

let mp = {};

/**
 * @param {any} state
 * @param {SimpleConfig} configuration
 * @param {Callback} callback
 */
function put(state, configuration, callback) {
  if (typeof configuration == "string") {
    mp[configuration] = state;
  }
  else if (configuration === null) {
    mp[id.getID(state)] = state;
  }
  else {
    mp[configuration.key] = state;
    // don't know what to do with gid yet.
  }
  callback(null, state);
};

/**
 * @param {any} state
 * @param {SimpleConfig} configuration
 * @param {Callback} callback
 */
function append(state, configuration, callback) {
  return callback(new Error('mem.append not implemented'));
};

/**
 * @param {SimpleConfig} configuration
 * @param {Callback} callback
 */
function get(configuration, callback) {
  if (typeof configuration == "string") {
    if (configuration in mp) {
      callback(null, mp[configuration]);
    }
    else {
      callback(new Error(`Given configuration not found in map: ${configuration}`));
    }
  }
  else {
    if (configuration.key in mp) {
      callback(null, mp[configuration.key]);
    }
    else {
      callback(new Error(`Given configuration not found in map: ${configuration}`));
    }
  }
}

/**
 * @param {SimpleConfig} configuration
 * @param {Callback} callback
 */
function del(configuration, callback) {
  if (typeof configuration == "string") {
    if (configuration in mp) {
      const r = mp[configuration];
      delete mp[configuration];
      callback(null, r);
    }
    else {
      callback(new Error(`Given configuration not found in map: ${configuration}`));
    }
  }
  else {
    if (configuration.key in mp) {
      const r = mp[configuration.key];
      delete mp[configuration.key];
      callback(null, r);
    }
    else {
      callback(new Error(`Given configuration not found in map: ${configuration}`));
    }
  }
};

module.exports = {put, get, del, append};
