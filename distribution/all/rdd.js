// @ts-check
'use strict';

/**
 * M7: Spark-Inspired Distributed Data Processing
 *
 * Data model: every internal RDD element is stored as {k: actualKey, v: actualValue}
 * under a UUID key in the distributed store. User functions always receive (key, value).
 *
 * User function signatures:
 *   map(fn):        fn(key, value) => [newKey, newValue]  (or {key, value})
 *   flatMap(fn):    fn(key, value) => [[k1,v1], [k2,v2], ...]
 *   filter(pred):   pred(key, value) => boolean
 *   reduceByKey(fn):fn(acc, val) => newAcc   (binary, associative recommended)
 *   reduce(fn,zero,cb): fn(acc, [key, value]) => newAcc
 *   foreach(fn,cb): fn(key, value) => void
 *
 * Serialization: user functions are serialized via fn.toString() and eval'd on workers.
 * Closures that reference outer variables not available at eval time will not work;
 * embed all needed data in the function body or as literals.
 *
 * sortByKey: distributed map phase runs on workers; final merge/sort runs on orchestrator.
 * Set ops and joins: materialize both sides on orchestrator, combine, redistribute.
 * Orchestrator materialization is documented and limited to the cases above.
 */

/* ─── UUID helper ─────────────────────────────────────────────────────────── */

function _uuid() {
  return Math.random().toString(36).slice(2, 9) + Date.now().toString(36);
}

/* ─── Temp group management ──────────────────────────────────────────────── */

/**
 * Create a temporary group with the same nodes as sourceGid.
 * Registers the group locally AND on every remote worker.
 */
function _createTempGroup(sourceGid, cb) {
  const tempGid = 'rdd' + _uuid();
  globalThis.distribution.local.groups.get(sourceGid, (e, nodes) => {
    if (e) { cb(e); return; }

    globalThis.distribution.local.groups.put({gid: tempGid}, nodes, (e2) => {
      if (e2) { cb(e2); return; }

      const nodeList = Object.values(nodes);
      if (nodeList.length === 0) { cb(null, tempGid, nodes); return; }

      let remaining = nodeList.length;
      for (const node of nodeList) {
        const remote = {node, service: 'groups', method: 'put'};
        globalThis.distribution.local.comm.send([{gid: tempGid}, nodes], remote, () => {
          // Ignore per-worker errors; local registration is authoritative.
          remaining--;
          if (remaining === 0) cb(null, tempGid, nodes);
        });
      }
    });
  });
}

/* ─── Store helpers ──────────────────────────────────────────────────────── */

/**
 * Store an array of MR results (each is {key: value}) into tempGid.
 * Returns the list of store keys.
 */
function _storeResults(results, tempGid, cb) {
  if (results.length === 0) { cb(null, []); return; }

  const newKeys = [];
  let done = 0;
  let failed = false;

  for (const obj of results) {
    const key = Object.keys(obj)[0];
    const val = obj[key];
    newKeys.push(key);
    globalThis.distribution[tempGid].store.put(val, key, (e) => {
      if (failed) return;
      if (e) { failed = true; cb(e); return; }
      if (++done === results.length) cb(null, newKeys);
    });
  }
}

/**
 * Store an array of [k, v] pairs as wrapped {k, v} objects under UUIDs.
 */
function _storeElements(elements, sourceGid, cb) {
  _createTempGroup(sourceGid, (e, tempGid) => {
    if (e) { cb(e); return; }
    if (elements.length === 0) { cb(null, tempGid, []); return; }

    const uuids = elements.map(_uuid);
    let done = 0;
    let failed = false;

    for (let i = 0; i < elements.length; i++) {
      const [k, v] = elements[i];
      globalThis.distribution[tempGid].store.put({k, v}, uuids[i], (e) => {
        if (failed) return;
        if (e) { failed = true; cb(e); return; }
        if (++done === elements.length) cb(null, tempGid, uuids);
      });
    }
  });
}

/**
 * Read all elements from gid using the given keys.
 * Returns [[k, v], ...] preserving key order.
 */
function _collectAll(gid, keys, isWrapped, cb) {
  if (keys.length === 0) { cb(null, []); return; }

  const results = new Array(keys.length).fill(null);
  let done = 0;
  let failed = false;

  for (let i = 0; i < keys.length; i++) {
    const idx = i;
    const key = keys[i];
    globalThis.distribution[gid].store.get(key, (e, v) => {
      if (failed) return;
      if (e) { failed = true; cb(e); return; }
      results[idx] = isWrapped ? [v.k, v.v] : [key, v];
      if (++done === keys.length) cb(null, results);
    });
  }
}

/* ─── Narrow ops code builder ────────────────────────────────────────────── */

function _buildNarrowOpsCode(narrowOps) {
  let code = '';
  for (const op of narrowOps) {
    if (op.type === 'map') {
      code += `
      {
        const _fn = ${op.fn.toString()};
        elems = elems.map(([k, v]) => {
          const r = _fn(k, v);
          if (Array.isArray(r)) return r;
          return [r.key !== undefined ? r.key : r.k, r.value !== undefined ? r.value : r.v];
        });
      }`;
    } else if (op.type === 'filter') {
      code += `
      {
        const _fn = ${op.fn.toString()};
        elems = elems.filter(([k, v]) => _fn(k, v));
      }`;
    } else if (op.type === 'flatMap') {
      code += `
      {
        const _fn = ${op.fn.toString()};
        elems = elems.flatMap(([k, v]) => {
          const rs = _fn(k, v);
          return rs.map(r => {
            if (Array.isArray(r)) return r;
            return [r.key !== undefined ? r.key : r.k, r.value !== undefined ? r.value : r.v];
          });
        });
      }`;
    }
  }
  return code;
}

/* ─── Mapper / reducer string builders ───────────────────────────────────── */

/** Narrow-only mapper: reads input, applies fused narrow ops, emits {uuid: {k,v}}. */
function _buildNarrowMapper(narrowOps, isInitial) {
  const extract = isInitial ? `let elems = [[key, value]];` : `let elems = [[value.k, value.v]];`;
  const ops = _buildNarrowOpsCode(narrowOps);
  return `function(key, value) {
    ${extract}
    ${ops}
    function _uuid() { return Math.random().toString(36).slice(2,9) + Date.now().toString(36); }
    return elems.map(([k, v]) => ({[_uuid()]: {k: k, v: v}}));
  }`;
}

/** Keyed mapper: reads input, applies narrow ops, emits {actualKey: value} for shuffle. */
function _buildKeyedMapper(narrowOps, isInitial) {
  const extract = isInitial ? `let elems = [[key, value]];` : `let elems = [[value.k, value.v]];`;
  const ops = _buildNarrowOpsCode(narrowOps);
  return `function(key, value) {
    ${extract}
    ${ops}
    return elems.map(([k, v]) => ({[k]: v}));
  }`;
}

/* ─── Core MR stage runner ───────────────────────────────────────────────── */

function _runMRStage(gid, keys, mapperStr, reducerStr, cb) {
  if (keys.length === 0) {
    _createTempGroup(gid, (e, tempGid) => {
      if (e) { cb(e); return; }
      cb(null, tempGid, []);
    });
    return;
  }

  const mapper = eval('(' + mapperStr + ')');
  const reducer = eval('(' + reducerStr + ')');

  globalThis.distribution[gid].mr.exec({keys, map: mapper, reduce: reducer}, (e, results) => {
    if (e) { cb(e); return; }
    _createTempGroup(gid, (e2, tempGid) => {
      if (e2) { cb(e2); return; }
      _storeResults(results, tempGid, (e3, newKeys) => {
        if (e3) { cb(e3); return; }
        cb(null, tempGid, newKeys);
      });
    });
  });
}

/* ─── Stage runners ──────────────────────────────────────────────────────── */

function _runNarrowStage(gid, keys, isWrapped, narrowOps, cb) {
  const mapperStr = _buildNarrowMapper(narrowOps, !isWrapped);
  const reducerStr = `function(key, values) { return {[key]: values[0]}; }`;
  _runMRStage(gid, keys, mapperStr, reducerStr, cb);
}

function _runReduceByKeyStage(gid, keys, isWrapped, narrowOps, fn, cb) {
  const mapperStr = _buildKeyedMapper(narrowOps, !isWrapped);
  const reducerStr = `function(key, values) {
    function _uuid() { return Math.random().toString(36).slice(2,9) + Date.now().toString(36); }
    const _fn = ${fn.toString()};
    const reduced = values.reduce((a, b) => _fn(a, b));
    return {[_uuid()]: {k: key, v: reduced}};
  }`;
  _runMRStage(gid, keys, mapperStr, reducerStr, cb);
}

function _runGroupByKeyStage(gid, keys, isWrapped, narrowOps, cb) {
  const mapperStr = _buildKeyedMapper(narrowOps, !isWrapped);
  const reducerStr = `function(key, values) {
    function _uuid() { return Math.random().toString(36).slice(2,9) + Date.now().toString(36); }
    return {[_uuid()]: {k: key, v: values}};
  }`;
  _runMRStage(gid, keys, mapperStr, reducerStr, cb);
}

function _runDistinctStage(gid, keys, isWrapped, narrowOps, opts, cb) {
  const byPair = opts && opts.byPair;
  const isInitial = !isWrapped;
  const extract = isInitial ? `let elems = [[key, value]];` : `let elems = [[value.k, value.v]];`;
  const opsCode = _buildNarrowOpsCode(narrowOps);

  let mapperStr;
  if (byPair) {
    mapperStr = `function(key, value) {
      ${extract}
      ${opsCode}
      return elems.map(([k, v]) => ({[JSON.stringify([k, v])]: {k: k, v: v}}));
    }`;
  } else {
    mapperStr = `function(key, value) {
      ${extract}
      ${opsCode}
      return elems.map(([k, v]) => ({[k]: {k: k, v: v}}));
    }`;
  }

  const reducerStr = `function(key, values) {
    function _uuid() { return Math.random().toString(36).slice(2,9) + Date.now().toString(36); }
    return {[_uuid()]: values[0]};
  }`;

  _runMRStage(gid, keys, mapperStr, reducerStr, cb);
}

/**
 * sortByKey: map phase runs on workers; orchestrator merges and sorts.
 * Elements are stored with positional keys (_s000000000000) to preserve order.
 */
function _runSortByKeyStage(gid, keys, isWrapped, narrowOps, ascending, cb) {
  _collectAfterNarrow(gid, keys, isWrapped, narrowOps, (e, elements) => {
    if (e) { cb(e); return; }

    elements.sort((a, b) => {
      if (a[0] < b[0]) return ascending ? -1 : 1;
      if (a[0] > b[0]) return ascending ? 1 : -1;
      return 0;
    });

    if (elements.length === 0) {
      _createTempGroup(gid, (e2, tempGid) => {
        if (e2) { cb(e2); return; }
        cb(null, tempGid, []);
      });
      return;
    }

    _createTempGroup(gid, (e2, tempGid) => {
      if (e2) { cb(e2); return; }

      const sortedKeys = elements.map((_, i) => '_s' + String(i).padStart(12, '0'));
      let done = 0;
      let failed = false;

      for (let i = 0; i < elements.length; i++) {
        const [k, v] = elements[i];
        globalThis.distribution[tempGid].store.put({k, v}, sortedKeys[i], (e3) => {
          if (failed) return;
          if (e3) { failed = true; cb(e3); return; }
          if (++done === elements.length) cb(null, tempGid, sortedKeys);
        });
      }
    });
  });
}

/** Collect elements after applying fused narrow ops (running an MR if there are ops). */
function _collectAfterNarrow(gid, keys, isWrapped, narrowOps, cb) {
  if (narrowOps.length === 0) {
    _collectAll(gid, keys, isWrapped, cb);
  } else {
    _runNarrowStage(gid, keys, isWrapped, narrowOps, (e, tempGid, tempKeys) => {
      if (e) { cb(e); return; }
      _collectAll(tempGid, tempKeys, true, cb);
    });
  }
}

/** Materialize another RDD (run its full pipeline and collect results). */
function _materialize(otherState, cb) {
  _runPipeline(otherState.gid, otherState.keys, false, otherState.pipeline, (e, fg, fk, fw) => {
    if (e) { cb(e); return; }
    _collectAll(fg, fk, fw, cb);
  });
}

/**
 * Binary ops (union, intersection, subtract, join, leftOuterJoin, rightOuterJoin).
 * Both sides are materialized on the orchestrator then combined.
 */
function _runBinaryStage(gid, keys, isWrapped, narrowOps, wideOp, cb) {
  _collectAfterNarrow(gid, keys, isWrapped, narrowOps, (e, leftElems) => {
    if (e) { cb(e); return; }

    _materialize(wideOp.other._state, (e2, rightElems) => {
      if (e2) { cb(e2); return; }

      let combined;
      const type = wideOp.type;

      if (type === 'union') {
        combined = [...leftElems, ...rightElems];
      } else if (type === 'intersection') {
        const rightKeys = new Set(rightElems.map(([k]) => k));
        combined = leftElems.filter(([k]) => rightKeys.has(k));
      } else if (type === 'subtract') {
        const rightKeys = new Set(rightElems.map(([k]) => k));
        combined = leftElems.filter(([k]) => !rightKeys.has(k));
      } else if (type === 'join') {
        combined = _innerJoin(leftElems, rightElems);
      } else if (type === 'leftOuterJoin') {
        combined = _outerJoin(leftElems, rightElems, 'left');
      } else if (type === 'rightOuterJoin') {
        combined = _outerJoin(leftElems, rightElems, 'right');
      } else {
        cb(new Error('Unknown binary op: ' + type));
        return;
      }

      _storeElements(combined, gid, (e3, tempGid, newKeys) => {
        if (e3) { cb(e3); return; }
        cb(null, tempGid, newKeys);
      });
    });
  });
}

function _innerJoin(left, right) {
  const rightMap = _groupByKey(right);
  const result = [];
  for (const [k, lv] of left) {
    if (rightMap[k]) {
      for (const rv of rightMap[k]) result.push([k, [lv, rv]]);
    }
  }
  return result;
}

function _outerJoin(left, right, side) {
  const result = [];
  if (side === 'left') {
    const rightMap = _groupByKey(right);
    for (const [k, lv] of left) {
      if (rightMap[k]) {
        for (const rv of rightMap[k]) result.push([k, [lv, rv]]);
      } else {
        result.push([k, [lv, null]]);
      }
    }
  } else {
    // rightOuterJoin: iterate right side, include all right elements
    const leftMap = _groupByKey(left);
    for (const [k, rv] of right) {
      if (leftMap[k]) {
        for (const lv of leftMap[k]) result.push([k, [lv, rv]]);
      } else {
        result.push([k, [null, rv]]);
      }
    }
  }
  return result;
}

function _groupByKey(elems) {
  const m = {};
  for (const [k, v] of elems) {
    if (!m[k]) m[k] = [];
    m[k].push(v);
  }
  return m;
}

/* ─── Pipeline execution ─────────────────────────────────────────────────── */

const NARROW_TYPES = new Set(['map', 'filter', 'flatMap']);

function _findStageEnd(pipeline) {
  for (let i = 0; i < pipeline.length; i++) {
    if (!NARROW_TYPES.has(pipeline[i].type)) return i;
  }
  return pipeline.length - 1;
}

function _runStage(gid, keys, isWrapped, stageOps, cb) {
  const lastOp = stageOps[stageOps.length - 1];
  const narrowOps = stageOps.filter(op => NARROW_TYPES.has(op.type));

  if (NARROW_TYPES.has(lastOp.type)) {
    _runNarrowStage(gid, keys, isWrapped, stageOps, cb);
  } else {
    _runWideStage(gid, keys, isWrapped, narrowOps, lastOp, cb);
  }
}

function _runWideStage(gid, keys, isWrapped, narrowOps, wideOp, cb) {
  switch (wideOp.type) {
    case 'reduceByKey':
      _runReduceByKeyStage(gid, keys, isWrapped, narrowOps, wideOp.fn, cb);
      break;
    case 'groupByKey':
      _runGroupByKeyStage(gid, keys, isWrapped, narrowOps, cb);
      break;
    case 'distinct':
      _runDistinctStage(gid, keys, isWrapped, narrowOps, wideOp.opts, cb);
      break;
    case 'sortByKey':
      _runSortByKeyStage(gid, keys, isWrapped, narrowOps, wideOp.opts.ascending !== false, cb);
      break;
    case 'union':
    case 'intersection':
    case 'subtract':
    case 'join':
    case 'leftOuterJoin':
    case 'rightOuterJoin':
      _runBinaryStage(gid, keys, isWrapped, narrowOps, wideOp, cb);
      break;
    default:
      cb(new Error('Unknown operation: ' + wideOp.type));
  }
}

function _runPipeline(gid, keys, isWrapped, pipeline, cb) {
  if (pipeline.length === 0) {
    cb(null, gid, keys, isWrapped);
    return;
  }

  const stageEnd = _findStageEnd(pipeline);
  const stageOps = pipeline.slice(0, stageEnd + 1);
  const remaining = pipeline.slice(stageEnd + 1);

  _runStage(gid, keys, isWrapped, stageOps, (e, newGid, newKeys) => {
    if (e) { cb(e); return; }
    _runPipeline(newGid, newKeys, true, remaining, cb);
  });
}

/* ─── Action execution ───────────────────────────────────────────────────── */

function _applyAction(gid, keys, isWrapped, action, opts, cb) {
  switch (action) {
    case 'collect':
      _collectAll(gid, keys, isWrapped, (e, elems) => {
        if (e) { cb(e); return; }
        cb(null, elems.map(([k, v]) => ({key: k, value: v})));
      });
      break;

    case 'count':
      cb(null, keys.length);
      break;

    case 'first':
      if (keys.length === 0) { cb(null, null); return; }
      _collectAll(gid, [keys[0]], isWrapped, (e, elems) => {
        if (e) { cb(e); return; }
        if (elems.length === 0) { cb(null, null); return; }
        cb(null, {key: elems[0][0], value: elems[0][1]});
      });
      break;

    case 'take':
      _collectAll(gid, keys.slice(0, opts.n), isWrapped, (e, elems) => {
        if (e) { cb(e); return; }
        cb(null, elems.map(([k, v]) => ({key: k, value: v})));
      });
      break;

    case 'reduce':
      _collectAll(gid, keys, isWrapped, (e, elems) => {
        if (e) { cb(e); return; }
        if (elems.length === 0) { cb(null, opts.zero); return; }
        let acc = opts.zero;
        for (const [k, v] of elems) {
          acc = opts.fn(acc, [k, v]);
        }
        cb(null, acc);
      });
      break;

    case 'foreach':
      _collectAll(gid, keys, isWrapped, (e, elems) => {
        if (e) { cb(e); return; }
        for (const [k, v] of elems) opts.fn(k, v);
        cb(null, null);
      });
      break;

    default:
      cb(new Error('Unknown action: ' + action));
  }
}

function _execute(state, action, opts, cb) {
  const {gid, keys, pipeline} = state;
  _runPipeline(gid, keys, false, pipeline, (e, fg, fk, fw) => {
    if (e) { cb(e); return; }
    _applyAction(fg, fk, fw, action, opts, cb);
  });
}

/* ─── RDD factory ────────────────────────────────────────────────────────── */

function _makeRDD(state) {
  return {
    _state: state,

    // Narrow transformations (lazy)
    map(fn) {
      return _makeRDD({...state, pipeline: [...state.pipeline, {type: 'map', fn}]});
    },
    flatMap(fn) {
      return _makeRDD({...state, pipeline: [...state.pipeline, {type: 'flatMap', fn}]});
    },
    filter(fn) {
      return _makeRDD({...state, pipeline: [...state.pipeline, {type: 'filter', fn}]});
    },

    // Wide transformations (lazy)
    distinct(opts = {}) {
      return _makeRDD({...state, pipeline: [...state.pipeline, {type: 'distinct', opts}]});
    },
    reduceByKey(fn) {
      return _makeRDD({...state, pipeline: [...state.pipeline, {type: 'reduceByKey', fn}]});
    },
    groupByKey() {
      return _makeRDD({...state, pipeline: [...state.pipeline, {type: 'groupByKey'}]});
    },
    sortByKey(opts = {}) {
      return _makeRDD({...state, pipeline: [...state.pipeline, {type: 'sortByKey', opts}]});
    },

    // Set operations (lazy, two-input)
    union(other) {
      return _makeRDD({...state, pipeline: [...state.pipeline, {type: 'union', other}]});
    },
    intersection(other) {
      return _makeRDD({...state, pipeline: [...state.pipeline, {type: 'intersection', other}]});
    },
    subtract(other) {
      return _makeRDD({...state, pipeline: [...state.pipeline, {type: 'subtract', other}]});
    },

    // Joins (lazy, two-input)
    join(other) {
      return _makeRDD({...state, pipeline: [...state.pipeline, {type: 'join', other}]});
    },
    leftOuterJoin(other) {
      return _makeRDD({...state, pipeline: [...state.pipeline, {type: 'leftOuterJoin', other}]});
    },
    rightOuterJoin(other) {
      return _makeRDD({...state, pipeline: [...state.pipeline, {type: 'rightOuterJoin', other}]});
    },

    // Actions (trigger execution)
    collect(cb) { _execute(state, 'collect', {}, cb); },
    count(cb) { _execute(state, 'count', {}, cb); },
    first(cb) { _execute(state, 'first', {}, cb); },
    take(n, cb) { _execute(state, 'take', {n}, cb); },
    reduce(fn, zero, cb) { _execute(state, 'reduce', {fn, zero}, cb); },
    foreach(fn, cb) { _execute(state, 'foreach', {fn}, cb); },
  };
}

/* ─── Service entry point ────────────────────────────────────────────────── */

/**
 * @param {Object} config - {gid: string}
 * @returns {{from: (keys: string[]) => object}}
 *
 * Usage:
 *   distribution[gid].rdd.from(keys).map(fn).filter(fn).collect(cb)
 *   distribution[gid].rdd.from(keys).reduceByKey(fn).collect(cb)
 */
function rdd(config) {
  const gid = config.gid || 'all';

  function from(keys) {
    return _makeRDD({gid, keys: keys || [], pipeline: []});
  }

  return {from};
}

module.exports = rdd;
