// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 * @typedef {import("../util/id.js").Node} Node
 *
 * @typedef {Object} Status
 * @property {(configuration: string, callback: Callback) => void} get
 * @property {(configuration: Node, callback: Callback) => void} spawn
 * @property {(callback: Callback) => void} stop
 */

/**
 * @param {Config} config
 * @returns {Status}
 */
function status(config) {
  const context = {};
  context.gid = config.gid || 'all';

  /**
   * @param {string} configuration
   * @param {Callback} callback
   */
  function get(configuration, callback) {
    globalThis.distribution.local.groups.get(context.gid, (e, v) => {
      if (e) {
        callback(e);
        return;
      }
      let total = Object.keys(v).length;
      if (total == 0) {
        callback(null, {});
        return;
      }
      let nodeToRes = {};
      // let aggregate = 0;
      // const isAggregate = configuration == "heapUsed" || configuration == "heapTotal";
      /** @type {Object.<string, Error>} */
      let nodeToError = {};
      for (const nodeSID in v) {
        const remote = {node: v[nodeSID], service: "status", method: "get"};
        globalThis.distribution.local.comm.send([configuration], remote, (e, v) => {
          if (e) {
            nodeToError[nodeSID] = e;
          }
          else {
            // if (isAggregate) {
            //   aggregate += v;
            // }
            // else {
            //   nodeToRes[nodeSID] = v;
            // }
            nodeToRes[nodeSID] = v;
          }
          total--;
          if (total == 0) {
            // callback(nodeToError, isAggregate ? aggregate : nodeToRes);
            callback(nodeToError, nodeToRes);
          }
        });
      }
    });
  }

  /**
   * @param {Node} configuration
   * @param {Callback} callback
   */
  function spawn(configuration, callback) {
    callback(new Error('status.spawn not implemented')); // If you won't implement this, check the skip.sh script.
  }

  /**
   * @param {Callback} callback
   */
  function stop(callback) {
    callback(new Error('status.stop not implemented')); // If you won't implement this, check the skip.sh script.
  }

  return {get, stop, spawn};
}

module.exports = status;
