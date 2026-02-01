// @ts-check

function serializeHelp(object) {
  if (typeof object == "number" || typeof object == "string" || typeof object == "boolean")
  {
    return { type: typeof object, value: String(object) };
  }
  else if (object === null) {
    return { type: "null", value: "" };
  }
  else if (object === undefined) {
    return { type: "undefined", value: "" };
  }
  else if (typeof object == "function") {
    return { type: "function", value: object.toString() };
  }
  else if (typeof object == "object") {
    // if (!object.constructor) {
    //   let res = {};
    //   for (key in object) {
    //     res[key] = serializeHelp(object[key])
    //   }
    //   return { type: "object", value: res };
    // }
    // const constructor = object.constructor.name;
    const constructor = object.constructor ? object.constructor.name : "Object";
    if (constructor == "Object") {
      let res = {};
      for (key in object) {
        res[key] = serializeHelp(object[key]);
      }
      return { type: "object", value: res };
    }
    else if (constructor == "Array") {
      let mp = {};
      for (var i = 0; i < object.length; i++) {
        mp[String(i)] = serializeHelp(object[i]);
      }
      return { type: "array", value: mp };
    }
    else if (constructor == "Date") {
      return { type: "date", value: object.toISOString() };
    }
    else if (constructor == "Error") {

      errorObj = { name: object.name, message: object.message, cause: object.cause };
      return { type: "error", value: serializeHelp(errorObj) };
    }
  }
}

/**
 * @param {any} object
 * @returns {string}
 */
function serialize(object) {
  return JSON.stringify(serializeHelp(object));
}


/**
 * @param {string} string
 * @returns {any}
 */
function deserialize(string) {
  if (typeof string !== 'string') {
    throw new Error(`Invalid argument type: ${typeof string}.`);
  }
  let obj;
  try {
    obj = JSON.parse(string);
  }
  catch (err) {
    throw new SyntaxError(`Invalid format: ${string}.`);
  }
  if (!("type" in obj) || !("value" in obj)) {
    throw new SyntaxError(`Invalid format: ${string}.`);
  }
  const t = obj.type;
  const v = obj.value;
  if (t == "number") {
    return Number(v);
  }
  else if (t == "string") {
    return v;
  }
  else if (t == "boolean") {
    if (v == "true") {
      return true;
    }
    else if (v == "false") {
      return false;
    }
    else {
      throw new Error(`Types don't match: ${string}.`);
    }
  }
  else if (t == "null") {
    return null;
  }
  else if (t == "undefined") {
    return undefined;
  }
  else if (t == "function") {
    return eval(`(${v})`);
  }
  else if (t == "object") {
    for (key in v) {
      v[key] = deserialize(JSON.stringify(v[key]));
    }
    return v;
  }
  else if (t == "array") {
    const res = Array(Object.keys(v).length);
    for (key in v) {
      res[Number(key)] = deserialize(JSON.stringify(v[key]));
    }
    return res;
  }
  else if (t == "date") {
    return new Date(v);
  }
  else if (t == "error") {
    const metadata = deserialize(JSON.stringify(v));
    let err = new Error(metadata.message);
    err.name = metadata.name;
    if (metadata.cause !== undefined) {
      err.cause = metadata.cause;
    }
    return err;
  }
}

module.exports = {
  serialize,
  deserialize,
};
