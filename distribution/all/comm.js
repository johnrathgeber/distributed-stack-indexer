// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 */

const { id } = require("../util/util.js");

/**
 * NOTE: This Target is slightly different from local.all.Target
 * @typedef {Object} Target
 * @property {string} service
 * @property {string} method
 * @property {string} [gid]
 *
 * @typedef {Object} Comm
 * @property {(message: any[], configuration: Target, callback: Callback) => void} send
 */

/**
 * @param {Config} config
 * @returns {Comm}
 */
function comm(config) {
  const context = {};
  context.gid = config.gid || 'all';

  /**
   * @param {any[]} message
   * @param {Target} configuration
   * @param {Callback} callback
   */
  function send(message, configuration, callback) {
    globalThis.distribution.local.groups.get(context.gid, (e, v) => {
      if (e) {
        callback(e);
        return;
      }
      let total = Object.keys(v).length;
      if (total == 0) {
        callback(new Error("Empty group."));
        return;
      }
      let nodeToRes = {};
      /** @type {Object.<string, Error>} */
      let nodeToError = {};
      for (const nodeSID in v) {
        const remote = {node: v[nodeSID], service: configuration.service, method: configuration.method};
        globalThis.distribution.local.comm.send(message, remote, (e, v) => {
          if (e) {
            nodeToError[nodeSID] = e;
          }
          else {
            nodeToRes[nodeSID] = v;
          }
          total--;
          if (total == 0) {
            callback(nodeToError, nodeToRes);
          }
        });
      }
    });
  }

  return {send};
}

module.exports = comm;
