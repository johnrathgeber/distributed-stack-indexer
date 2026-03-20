// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Node} Node
 */

const http = require('node:http');
const { Http2ServerRequest } = require("node:http2");
const { options } = require("yargs");

/**
 * @typedef {Object} Target
 * @property {string} service
 * @property {string} method
 * @property {Node} node
 * @property {string} [gid]
 */

/**
 * @param {Array<any>} message
 * @param {Target} remote
 * @param {(error: Error, value?: any) => void} callback
 * @returns {void}
 */
function send(message, remote, callback) {
  if (!(Array.isArray(message))) {
    callback(new Error(`Message parameter must be an array.`));
    return;
  }
  const serv = remote.service;
  const meth = remote.method;
  const node = remote.node;
  if (!serv) {
    callback(new Error(`Did not receive server.`));
    return;
  }
  if (!meth) {
    callback(new Error(`Did not receive method.`));
    return;
  }
  if (!node) {
    callback(new Error(`Did not receive node.`));
    return;
  }
  if (!node.ip) {
    callback(new Error(`Did not receive node ip.`));
    return;
  }
  if (!node.port) {
    callback(new Error(`Did not receive node port.`));
    return;
  }
  const gid = remote.gid;
  const path = gid? `/${gid}/${serv}/${meth}` : `/local/${serv}/${meth}`;
  const options = {
    hostname: node.ip,
    port: node.port,
    path: path,
    method: 'PUT',
  };
  const req = http.request(options, (res) => {
    let responseBody = "";
    res.on('data', (chunk) => {
      responseBody += chunk;
    });
    res.on('end', () => {
      try {
        const result = distribution.util.deserialize(responseBody);
        if (Array.isArray(result) && result.length == 2) {
          const [e, v] = result;
          if (e) {
            callback(e);
            return;
          }
          callback(null, v);
        }
        else {
          callback(new Error(`Did not receive array of length 2 from node.js, got: ${result}`));
        }
      }
      catch (e) {
        callback(e);
      }
    });
  });
  req.on('error', (e) => {
    callback(e);
  });
  req.write(distribution.util.serialize(message));
  req.end();
}

module.exports = {send};
