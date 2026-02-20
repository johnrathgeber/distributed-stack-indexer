// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 * @typedef {import("../util/id.js").Node} Node
 *
 * @typedef {Object} Groups
 * @property {(config: Config | string, group: Object.<string, Node>, callback: Callback) => void} put
 * @property {(name: string, callback: Callback) => void} del
 * @property {(name: string, callback: Callback) => void} get
 * @property {(name: string, node: Node, callback: Callback) => void} add
 * @property {(name: string, node: string, callback: Callback) => void} rem
 */

/**
 * @param {Config} config
 * @returns {Groups}
 */
function groups(config) {
  const context = {gid: config.gid || 'all'};

  /**
   * @param {Config | string} config
   * @param {Object.<string, Node>} group
   * @param {Callback} callback
   */
  function put(config, group, callback) {
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
        const remote = {node: v[nodeSID], service: "groups", method: "put"};
        globalThis.distribution.local.comm.send([config, group], remote, (e, v) => {
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
   * @param {string} name
   * @param {Callback} callback
   */
  function del(name, callback) {
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
        const remote = {node: v[nodeSID], service: "groups", method: "del"};
        globalThis.distribution.local.comm.send([name], remote, (e, v) => {
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
   * @param {string} name
   * @param {Callback} callback
   */
  function get(name, callback) {
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
        const remote = {node: v[nodeSID], service: "groups", method: "get"};
        globalThis.distribution.local.comm.send([name], remote, (e, v) => {
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
   * @param {string} name
   * @param {Node} node
   * @param {Callback} callback
   */
  function add(name, node, callback) {
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
        const remote = {node: v[nodeSID], service: "groups", method: "add"};
        globalThis.distribution.local.comm.send([name, node], remote, (e, v) => {
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
   * @param {string} name
   * @param {string} node
   * @param {Callback} callback
   */
  function rem(name, node, callback) {
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
        const remote = {node: v[nodeSID], service: "groups", method: "rem"};
        globalThis.distribution.local.comm.send([name, node], remote, (e, v) => {
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

  return {
    put, del, get, add, rem,
  };
}

module.exports = groups;
