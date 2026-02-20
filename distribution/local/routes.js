/**
 * @typedef {import("../types").Callback} Callback
 * @typedef {string} ServiceName
 */

const distribution = require("../../distribution");

let mp = {};


/**
 * @param {ServiceName | {service: ServiceName, gid?: string}} configuration
 * @param {Callback} callback
 * @returns {void}
 */
function get(configuration, callback) {
  if (typeof configuration == "string") {
    if (configuration in mp) {
      callback(null, mp[configuration]);
    }
    else {
      callback(new Error("No corresponding service found."));
    }
  }
  else {
    if (configuration.gid && configuration.gid == "local") {
      if (configuration.service in mp) {
        callback(null, mp[configuration.service]);
      }
      else {
        callback(new Error("No corresponding service found."));
      }
    }
    else {
      const service = globalThis.distribution[configuration.gid]?.[configuration.service];
      if (service) {
        callback(null, service);
      }
      else {
        callback(new Error("No corresponding service found."));
      }
    }
  }
}

/**
 * @param {object} service
 * @param {string} configuration
 * @param {Callback} callback
 * @returns {void}
 */
function put(service, configuration, callback) {
  mp[configuration] = service;
  callback(null, null);
}

/**
 * @param {string} configuration
 * @param {Callback} callback
 */
function rem(configuration, callback) {
  if (configuration in mp) {
    const serv = mp[configuration];
    delete mp[configuration];
    callback(null, serv);
  }
  else {
    callback(new Error("Service not found."));
  }
}

module.exports = {get, put, rem};
