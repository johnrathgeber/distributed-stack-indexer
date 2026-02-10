// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Node} Node
 */

const util = require("../util/util.js");

/**
 * @param {string} configuration
 * @param {Callback} callback
 */
function get(configuration, callback) {
  const config = globalThis.distribution.node.config;
  if (configuration == "nid") {
    callback(null, util.id.getNID(config));
  }
  else if (configuration == "sid") {
    callback(null, util.id.getSID(config));
  }
  else if (configuration == "ip") {
    callback(null, config.ip);
  }
  else if (configuration == "port") {
    callback(null, config.port);
  }
  else if (configuration == "counts") {
    callback(null, globalThis.distribution.node.count ? globalThis.distribution.node.count : 0);
  }
  else if (configuration == "heapTotal") {
    callback(null, process.memoryUsage().heapTotal);
  }
  else if (configuration == "heapUsed") {
    callback(null, process.memoryUsage().heapUsed);
  }
  else {
    callback(new Error(`status.get() argument not supported: ${JSON.stringify(configuration)}`));
  }
};


/**
 * @param {Node} configuration
 * @param {Callback} callback
 */
function spawn(configuration, callback) {
  callback(new Error('status.spawn not implemented'));
}

/**
 * @param {Callback} callback
 */
function stop(callback) {
  callback(new Error('status.stop not implemented'));
}

module.exports = {get, spawn, stop};
