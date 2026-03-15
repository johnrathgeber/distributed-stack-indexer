// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 *
 * @typedef {Object} StoreConfig
 * @property {?string} key
 * @property {?string} gid
 *
 * @typedef {StoreConfig | string | null} SimpleConfig
 */

/* Notes/Tips:

- Use absolute paths to make sure they are agnostic to where your code is running from!
  Use the `path` module for that.
*/

const { id } = require("../util/util.js");
const fs = require('node:fs');
const path = require('node:path');
const util = require("../util/util.js");

/**
 * @param {any} state
 * @param {SimpleConfig} configuration
 * @param {Callback} callback
 */
function put(state, configuration, callback) {
  const serialized = util.serialize(state);
  if (typeof configuration == "string") {
    const alphanumeric = configuration.replace(/[^a-zA-Z0-9]/g, '');
    const filePath = path.resolve("store", alphanumeric);
    fs.writeFile(filePath, serialized, (e) => {
      if (e) {
        callback(new Error(`Error while putting a file into store: ${e.message}`));
      }
      else {
        callback(null, state);
      }
    });
  }
  else if (configuration === null) {
    const alphanumeric = id.getID(state).replace(/[^a-zA-Z0-9]/g, '');
    const filePath = path.resolve("store", alphanumeric);
    fs.writeFile(filePath, serialized, (e) => {
      if (e) {
        callback(new Error(`Error while putting a file into store: ${e.message}`));
      }
      else {
        callback(null, state);
      }
    });
  }
  else {
    const alphanumeric = (configuration.key).replace(/[^a-zA-Z0-9]/g, '');
    const filePath = path.resolve("store", configuration.gid, alphanumeric);
    fs.mkdirSync(path.resolve("store", configuration.gid), {recursive: true});
    fs.writeFile(filePath, serialized, (e) => {
      if (e) {
        callback(new Error(`Error while putting a file into store: ${e.message}`));
      }
      else {
        callback(null, state);
      }
    });
  }
}

/**
 * @param {SimpleConfig} configuration
 * @param {Callback} callback
 */
function get(configuration, callback) {
  if (typeof configuration == "string") {
    try {
      const alphanumeric = configuration.replace(/[^a-zA-Z0-9]/g, '');
      const filePath = path.resolve("store", alphanumeric);
      const data = fs.readFileSync(filePath, 'utf8');
      callback(null, util.deserialize(data));
    }
    catch (e) {
      callback(new Error(`Error while getting a file from store: ${e.message}`));
    }
  }
  else {
    if (configuration.key === null) {
      const dirPath = path.resolve('store', configuration.gid);
      fs.readdir(dirPath, (e, files) => {
        if (e) {
          callback(null, []);
        }
        else {
          callback(null, files);
        }
      });
      return;
    }
    try {
      const alphanumeric = (configuration.key).replace(/[^a-zA-Z0-9]/g, '');
      const filePath = path.resolve("store", configuration.gid, alphanumeric);
      const data = fs.readFileSync(filePath, 'utf8');
      callback(null, util.deserialize(data));
    }
    catch (e) {
      callback(new Error(`Error while getting a file from store: ${e.message}`));
    }
  }
}

/**
 * @param {SimpleConfig} configuration
 * @param {Callback} callback
 */
function del(configuration, callback) {
  if (typeof configuration == "string") {
    try {
      const alphanumeric = configuration.replace(/[^a-zA-Z0-9]/g, '');
      const filePath = path.resolve("store", alphanumeric);
      const data = fs.readFileSync(filePath, 'utf8');
      fs.unlinkSync(filePath);
      callback(null, util.deserialize(data));
    }
    catch (e) {
      callback(new Error(`Error while deleting a file from store: ${e.message}`));
    }
  }
  else {
    try {
      const alphanumeric = (configuration.key).replace(/[^a-zA-Z0-9]/g, '');
      const filePath = path.resolve("store", configuration.gid, alphanumeric);
      const data = fs.readFileSync(filePath, 'utf8');
      fs.unlinkSync(filePath);
      callback(null, util.deserialize(data));
    }
    catch (e) {
      callback(new Error(`Error while deleting a file from store: ${e.message}`));
    }
  }
}

/**
 * @param {any} state
 * @param {SimpleConfig} configuration
 * @param {Callback} callback
 */
function append(state, configuration, callback) {                                                                       
  get(configuration, (e, v) => {                                                                                      
    const arr = e ? [] : v;                                                                                             
    arr.push(state);                                                                                                  
    const serialized = util.serialize(arr);                                                                             
    try {                                                                                                               
      let filePath;
      if (typeof configuration === 'string') {
        const alphanumeric = configuration.replace(/[^a-zA-Z0-9]/g, '');
        filePath = path.resolve('store', alphanumeric);
      }
      else if (configuration === null) {
        const alphanumeric = id.getID(state).replace(/[^a-zA-Z0-9]/g, '');
        filePath = path.resolve('store', alphanumeric);
      }
      else {
        const alphanumeric = (configuration.key).replace(/[^a-zA-Z0-9]/g, '');
        fs.mkdirSync(path.resolve('store', configuration.gid), {recursive: true});
        filePath = path.resolve('store', configuration.gid, alphanumeric);
      }
      fs.writeFileSync(filePath, serialized);
      callback(null, arr);
    } catch (err) {
      callback(new Error(`Error while appending: ${err.message}`));
    }
  });
}

module.exports = {put, get, del, append};
