// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 * @typedef {import("../util/id.js").NID} NID
 */

const { id } = require("../util/util.js");

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
 * @typedef {Object} Mr
 * @property {(configuration: MRConfig, cb: Callback) => void} exec
 */


/*
  Note: The only method explicitly exposed in the `mr` service is `exec`.
  Other methods, such as `map`, `shuffle`, and `reduce`, should be dynamically
  installed on the remote nodes and not necessarily exposed to the user.
*/

/**
 * @param {Config} config
 * @returns {Mr}
 */
function mr(config) {
  const context = {
    gid: config.gid || 'all',
  };

  /**
   * @param {MRConfig} configuration
   * @param {Callback} cb
   * @returns {void}
   */
  function exec(configuration, cb) {
    function generateServiceName() {
      const characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz01223456789";
      let result = "";
      for (let i = 0; i < 7; i++) {
        result += characters.charAt(Math.floor(Math.random() * characters.length));
      }
      return "mr-" + result;
    }
    let servicesCreated = [];
    globalThis.distribution.local.groups.get(context.gid, (e, v) => {
      if (e) {
        cb(e);
        return;
      }
      let total = Object.keys(v).length;
      if (total == 0) {
        cb(null, []);
        return;
      }
      const servName = generateServiceName();
      servicesCreated.push(servName);
      const coordNode = globalThis.distribution.node.config;
      let serv = {};
      let cntr = 0;
      function notify(cb2) {
        cb2(null, {});
        cntr++;
        if (cntr === total) {
          cntr = 0;
          const nodeNIDs = Object.values(v).map(x => id.getNID(x));
          for (const nodeSID in v) {
            const remote = {node: v[nodeSID], service: "mr", method: "mapIt"};
            globalThis.distribution.local.comm.send([nodeNIDs, v], remote, (e, v2) => {
              if (e) {
                cb(e);
                return;
              }
              cntr++;
              if (cntr === total) {
                cntr = 0;
                let newKeys = [];
                for (const nodeSID in v) {
                  const remote = {node: v[nodeSID], service: "mr", method: "shuffleIt"};
                  globalThis.distribution.local.comm.send([nodeNIDs, v], remote, (e, v2) => {
                      if (e) {
                        cb(e);
                        return;
                      }
                      newKeys = newKeys.concat(v2);
                      cntr++;
                      if (cntr === total) {
                        cntr = 0;
                        let toRtn = [];
                        newKeys = [...new Set(newKeys)];
                        for (const nodeSID in v) {
                          const remote = {node: v[nodeSID], service: "mr", method: "reduceIt"};
                          globalThis.distribution.local.comm.send([newKeys, nodeNIDs, v], remote, (e, v2) => {
                              if (e) {
                                cb(e);
                                return;
                              }
                              toRtn = toRtn.concat(v2 || []);
                              cntr++;
                              if (cntr === total) {
                                const total2 = servicesCreated.length;
                                let cntr2 = 0;
                                for (const serv of servicesCreated) {
                                  const remote = {node: coordNode, service: "routes", method: "rem"};
                                  globalThis.distribution.local.comm.send([serv], remote, (e, v2) => {
                                    if (e) {
                                      cb(e);
                                      return;
                                    }
                                    cntr2++;
                                    if (cntr2 === total2) {
                                      cb(null, toRtn);
                                    }
                                  });
                                }
                              }
                            });
                        }
                      }
                    });
                  }
                }
              });
            }
          }
        }
      serv.notify = notify;
      globalThis.distribution.local.routes.put(serv, servName, (e, v2) => {
        if (e) {
          cb(e);
          return;
        }
        for (const nodeSID in v) {
          const remote = {node: v[nodeSID], service: "mr", method: "recvMapReduce"};
          globalThis.distribution.local.comm.send([configuration, coordNode, servName, context.gid], remote, (e, v) => {
            if (e) {
              cb(e);
              return;
            }
          });
        }
      });
    });
  }

  return {exec};
}

module.exports = mr;
