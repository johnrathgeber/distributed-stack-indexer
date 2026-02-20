// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 *
 * @typedef {Object} Routes
 * @property {(service: object, name: string, callback: Callback) => void} put
 * @property {(configuration: string, callback: Callback) => void} rem
 */

/**
 * @param {Config} config
 * @returns {Routes}
 */
function routes(config) {
  const context = {};
  context.gid = config.gid || 'all';

  /**
   * @param {object} service
   * @param {string} name
   * @param {Callback} callback
   */
  function put(service, name, callback) {
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
      /** @type {Object.<string, Error>} */
      let nodeToError = {};
      for (const nodeSID in v) {
        const remote = {node: v[nodeSID], service: "routes", method: "put"};
        globalThis.distribution.local.comm.send([service, name], remote, (e, v) => {
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

  /**
   * @param {string} configuration
   * @param {Callback} callback
   */
  function rem(configuration, callback) {
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
      /** @type {Object.<string, Error>} */
      let nodeToError = {};
      for (const nodeSID in v) {
        const remote = {node: v[nodeSID], service: "routes", method: "rem"};
        globalThis.distribution.local.comm.send([configuration], remote, (e, v) => {
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

  return {put, rem};
}

module.exports = routes;
