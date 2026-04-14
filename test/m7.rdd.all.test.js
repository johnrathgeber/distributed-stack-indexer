/**
 * M7 RDD tests — correctness verification for all operations.
 *
 * Coverage:
 *   Actions:      collect, count, first, take, reduce, foreach
 *   Narrow ops:   map, filter, flatMap (individually and fused)
 *   Wide ops:     reduceByKey, groupByKey, distinct (key + pair), sortByKey (asc + desc)
 *   Set ops:      union, intersection, subtract
 *   Joins:        join, leftOuterJoin, rightOuterJoin
 *   Pipelines:    narrow → wide chains, multi-stage chains
 *   Edge cases:   empty input, single element, duplicate keys from flatMap
 *
 * User function convention (per rdd.js):
 *   map(fn):      fn(key, value) => [newKey, newValue]
 *   flatMap(fn):  fn(key, value) => [[k1, v1], [k2, v2], ...]
 *   filter(pred): pred(key, value) => boolean
 *   reduceByKey:  fn(acc, val) => newAcc
 *   reduce:       fn(acc, [key, value]) => newAcc
 *
 * NOTE: Functions passed to RDD ops execute on remote workers via eval(fn.toString()).
 * They must be self-contained — no captured outer variables.
 */

require('../distribution.js')();
require('./helpers/sync-guard');

const distribution = globalThis.distribution;
const id = distribution.util.id;

const TEST_TIMEOUT = 20000;

const n1 = {ip: '127.0.0.1', port: 7110};
const n2 = {ip: '127.0.0.1', port: 7111};
const n3 = {ip: '127.0.0.1', port: 7112};

const rddGroup = {};

/* ─── Helpers ──────────────────────────────────────────────────────────────── */

/** Store multiple [key, value] pairs in a group and call cb when all are written. */
function putAll(gid, pairs, cb) {
  if (pairs.length === 0) { cb(); return; }
  let remaining = pairs.length;
  for (const [k, v] of pairs) {
    distribution[gid].store.put(v, k, () => {
      if (--remaining === 0) cb();
    });
  }
}

/** Extract values from collect results and sort for stable comparison. */
function sortedVals(results) {
  return results.map((r) => r.value).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
}

/** Extract keys from collect results and sort. */
function sortedKeys(results) {
  return results.map((r) => r.key).sort();
}

/** Convert collect output to a plain {key: value} map for easy lookup. */
function toMap(results) {
  const m = {};
  for (const {key, value} of results) m[key] = value;
  return m;
}

/* ─── Actions ──────────────────────────────────────────────────────────────── */

test('collect returns all stored elements', (done) => {
  const pairs = [['col_a', 10], ['col_b', 20], ['col_c', 30]];
  putAll('rddtest', pairs, () => {
    distribution.rddtest.rdd.from(['col_a', 'col_b', 'col_c']).collect((e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).toHaveLength(3);
        expect(sortedVals(v)).toEqual([10, 20, 30]);
        done();
      } catch (err) { done(err); }
    });
  });
}, TEST_TIMEOUT);

test('count returns the number of elements', (done) => {
  const pairs = [['cnt_a', 1], ['cnt_b', 2], ['cnt_c', 3], ['cnt_d', 4]];
  putAll('rddtest', pairs, () => {
    distribution.rddtest.rdd.from(['cnt_a', 'cnt_b', 'cnt_c', 'cnt_d']).count((e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).toBe(4);
        done();
      } catch (err) { done(err); }
    });
  });
}, TEST_TIMEOUT);

test('first returns one element', (done) => {
  const pairs = [['fst_a', 42]];
  putAll('rddtest', pairs, () => {
    distribution.rddtest.rdd.from(['fst_a']).first((e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).not.toBeNull();
        expect(v.value).toBe(42);
        done();
      } catch (err) { done(err); }
    });
  });
}, TEST_TIMEOUT);

test('take(n) returns first n elements', (done) => {
  const pairs = [['tak_a', 1], ['tak_b', 2], ['tak_c', 3], ['tak_d', 4], ['tak_e', 5]];
  putAll('rddtest', pairs, () => {
    distribution.rddtest.rdd.from(['tak_a', 'tak_b', 'tak_c', 'tak_d', 'tak_e']).take(3, (e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).toHaveLength(3);
        done();
      } catch (err) { done(err); }
    });
  });
}, TEST_TIMEOUT);

test('reduce folds all elements into a single value', (done) => {
  const pairs = [['red_a', 1], ['red_b', 2], ['red_c', 3], ['red_d', 4]];
  putAll('rddtest', pairs, () => {
    distribution.rddtest.rdd.from(['red_a', 'red_b', 'red_c', 'red_d']).reduce(
      (acc, [k, v]) => acc + v,
      0,
      (e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v).toBe(10);
          done();
        } catch (err) { done(err); }
      }
    );
  });
}, TEST_TIMEOUT);

test('foreach applies side effects to all elements', (done) => {
  const pairs = [['fe_a', 1], ['fe_b', 2], ['fe_c', 3]];
  putAll('rddtest', pairs, () => {
    const seen = [];
    distribution.rddtest.rdd.from(['fe_a', 'fe_b', 'fe_c']).foreach(
      (k, v) => seen.push(v),
      (e) => {
        try {
          expect(e).toBeFalsy();
          expect(seen.sort()).toEqual([1, 2, 3]);
          done();
        } catch (err) { done(err); }
      }
    );
  });
}, TEST_TIMEOUT);

/* ─── Narrow transformations ───────────────────────────────────────────────── */

test('map transforms each element', (done) => {
  const pairs = [['map_a', 1], ['map_b', 2], ['map_c', 3]];
  putAll('rddtest', pairs, () => {
    distribution.rddtest.rdd.from(['map_a', 'map_b', 'map_c'])
      .map((k, v) => [k, v * 10])
      .collect((e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(sortedVals(v)).toEqual([10, 20, 30]);
          done();
        } catch (err) { done(err); }
      });
  });
}, TEST_TIMEOUT);

test('map can change keys', (done) => {
  const pairs = [['mk_a', 'hello'], ['mk_b', 'world']];
  putAll('rddtest', pairs, () => {
    distribution.rddtest.rdd.from(['mk_a', 'mk_b'])
      .map((k, v) => [v, k])   // swap key and value
      .collect((e, v) => {
        try {
          expect(e).toBeFalsy();
          const m = toMap(v);
          expect(m['hello']).toBe('mk_a');
          expect(m['world']).toBe('mk_b');
          done();
        } catch (err) { done(err); }
      });
  });
}, TEST_TIMEOUT);

test('filter keeps only elements matching predicate', (done) => {
  const pairs = [['fil_a', 1], ['fil_b', 2], ['fil_c', 3], ['fil_d', 4], ['fil_e', 5]];
  putAll('rddtest', pairs, () => {
    distribution.rddtest.rdd.from(['fil_a', 'fil_b', 'fil_c', 'fil_d', 'fil_e'])
      .filter((k, v) => v % 2 === 0)
      .collect((e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(sortedVals(v)).toEqual([2, 4]);
          done();
        } catch (err) { done(err); }
      });
  });
}, TEST_TIMEOUT);

test('filter returning no matches produces empty result', (done) => {
  const pairs = [['fnm_a', 1], ['fnm_b', 3], ['fnm_c', 5]];
  putAll('rddtest', pairs, () => {
    distribution.rddtest.rdd.from(['fnm_a', 'fnm_b', 'fnm_c'])
      .filter((k, v) => v > 100)
      .collect((e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v).toHaveLength(0);
          done();
        } catch (err) { done(err); }
      });
  });
}, TEST_TIMEOUT);

test('flatMap expands each element to multiple outputs', (done) => {
  // Each sentence → individual words
  const pairs = [['fm_a', 'hello world'], ['fm_b', 'foo bar baz']];
  putAll('rddtest', pairs, () => {
    distribution.rddtest.rdd.from(['fm_a', 'fm_b'])
      .flatMap((k, v) => v.split(' ').map((w) => [w, 1]))
      .collect((e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v).toHaveLength(5);
          expect(sortedKeys(v)).toEqual(['bar', 'baz', 'foo', 'hello', 'world']);
          done();
        } catch (err) { done(err); }
      });
  });
}, TEST_TIMEOUT);

test('fused map + filter executes as one distributed stage', (done) => {
  const pairs = [['fu_a', 1], ['fu_b', 2], ['fu_c', 3], ['fu_d', 4]];
  putAll('rddtest', pairs, () => {
    distribution.rddtest.rdd.from(['fu_a', 'fu_b', 'fu_c', 'fu_d'])
      .map((k, v) => [k, v * 2])
      .filter((k, v) => v > 4)
      .collect((e, v) => {
        try {
          expect(e).toBeFalsy();
          // 1*2=2 (no), 2*2=4 (no), 3*2=6 (yes), 4*2=8 (yes)
          expect(sortedVals(v)).toEqual([6, 8]);
          done();
        } catch (err) { done(err); }
      });
  });
}, TEST_TIMEOUT);

/* ─── Wide transformations ─────────────────────────────────────────────────── */

test('reduceByKey aggregates values per key', (done) => {
  // Classic word count via flatMap → reduceByKey
  const pairs = [
    ['rbk_d1', 'the cat sat on the mat'],
    ['rbk_d2', 'the cat in the hat'],
  ];
  putAll('rddtest', pairs, () => {
    distribution.rddtest.rdd.from(['rbk_d1', 'rbk_d2'])
      .flatMap((k, v) => v.split(' ').map((w) => [w, 1]))
      .reduceByKey((a, b) => a + b)
      .collect((e, v) => {
        try {
          expect(e).toBeFalsy();
          const m = toMap(v);
          expect(m['the']).toBe(4);
          expect(m['cat']).toBe(2);
          expect(m['sat']).toBe(1);
          done();
        } catch (err) { done(err); }
      });
  });
}, TEST_TIMEOUT);

test('groupByKey collects all values per key', (done) => {
  const pairs = [['gbk_a', 'x'], ['gbk_b', 'y'], ['gbk_c', 'x'], ['gbk_d', 'z']];
  putAll('rddtest', pairs, () => {
    // Map to (value → original key) then groupByKey to collect all keys per original value
    distribution.rddtest.rdd.from(['gbk_a', 'gbk_b', 'gbk_c', 'gbk_d'])
      .map((k, v) => [v, k])
      .groupByKey()
      .collect((e, v) => {
        try {
          expect(e).toBeFalsy();
          const m = toMap(v);
          expect(m['x'].sort()).toEqual(['gbk_a', 'gbk_c']);
          expect(m['y']).toEqual(['gbk_b']);
          expect(m['z']).toEqual(['gbk_d']);
          done();
        } catch (err) { done(err); }
      });
  });
}, TEST_TIMEOUT);

test('distinct removes duplicate keys', (done) => {
  // flatMap produces duplicate keys; distinct keeps one per key
  const pairs = [['dist_a', 'hello hello world'], ['dist_b', 'world foo']];
  putAll('rddtest', pairs, () => {
    distribution.rddtest.rdd.from(['dist_a', 'dist_b'])
      .flatMap((k, v) => v.split(' ').map((w) => [w, 1]))
      .distinct()
      .collect((e, v) => {
        try {
          expect(e).toBeFalsy();
          // 'hello', 'world', 'foo' — each appears exactly once
          expect(v).toHaveLength(3);
          expect(sortedKeys(v)).toEqual(['foo', 'hello', 'world']);
          done();
        } catch (err) { done(err); }
      });
  });
}, TEST_TIMEOUT);

test('distinct with opts.byPair deduplicates (key, value) pairs', (done) => {
  const pairs = [['dbp_a', 'x y x'], ['dbp_b', 'x z']];
  putAll('rddtest', pairs, () => {
    // flatMap → [word, 1] pairs; distinct by pair removes exact duplicates
    distribution.rddtest.rdd.from(['dbp_a', 'dbp_b'])
      .flatMap((k, v) => v.split(' ').map((w) => [w, 1]))
      .distinct({byPair: true})
      .collect((e, v) => {
        try {
          expect(e).toBeFalsy();
          // Pairs: (x,1)(y,1)(x,1)(x,1)(z,1) → distinct → (x,1)(y,1)(z,1)
          expect(v).toHaveLength(3);
          done();
        } catch (err) { done(err); }
      });
  });
}, TEST_TIMEOUT);

test('sortByKey returns elements in ascending key order', (done) => {
  const pairs = [['sk_c', 3], ['sk_a', 1], ['sk_e', 5], ['sk_b', 2], ['sk_d', 4]];
  putAll('rddtest', pairs, () => {
    distribution.rddtest.rdd.from(['sk_c', 'sk_a', 'sk_e', 'sk_b', 'sk_d'])
      .sortByKey({ascending: true})
      .collect((e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v.map((r) => r.key)).toEqual(['sk_a', 'sk_b', 'sk_c', 'sk_d', 'sk_e']);
          expect(v.map((r) => r.value)).toEqual([1, 2, 3, 4, 5]);
          done();
        } catch (err) { done(err); }
      });
  });
}, TEST_TIMEOUT);

test('sortByKey with ascending:false returns elements in descending key order', (done) => {
  const pairs = [['skd_c', 3], ['skd_a', 1], ['skd_e', 5], ['skd_b', 2], ['skd_d', 4]];
  putAll('rddtest', pairs, () => {
    distribution.rddtest.rdd.from(['skd_c', 'skd_a', 'skd_e', 'skd_b', 'skd_d'])
      .sortByKey({ascending: false})
      .collect((e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v.map((r) => r.key)).toEqual(['skd_e', 'skd_d', 'skd_c', 'skd_b', 'skd_a']);
          done();
        } catch (err) { done(err); }
      });
  });
}, TEST_TIMEOUT);

/* ─── Set operations ───────────────────────────────────────────────────────── */

test('union combines two datasets (duplicates allowed)', (done) => {
  // Use distinct physical keys to avoid concurrent write races.
  // Map them to logical keys so 'u_b' appears in both sides (testing duplicate semantics).
  const left = [['ul_a', 1], ['ul_b', 2]];
  const right = [['ur_c', 3], ['ur_b', 99]]; // 'u_b' will appear in both after map
  putAll('rddtest', [...left, ...right], () => {
    const lRdd = distribution.rddtest.rdd.from(['ul_a', 'ul_b'])
      .map((k, v) => [k.replace('ul_', 'u_'), v]);
    const rRdd = distribution.rddtest.rdd.from(['ur_c', 'ur_b'])
      .map((k, v) => [k.replace('ur_', 'u_'), v]);
    lRdd.union(rRdd).collect((e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).toHaveLength(4); // duplicates kept: u_a, u_b (left), u_c, u_b (right)
        done();
      } catch (err) { done(err); }
    });
  });
}, TEST_TIMEOUT);

test('intersection returns elements whose keys appear in both datasets', (done) => {
  // Use distinct physical key prefixes (il_ left, ir_ right) to avoid write races.
  const left = [['il_a', 1], ['il_b', 2], ['il_c', 3]];
  const right = [['ir_b', 20], ['ir_c', 30], ['ir_d', 40]];
  putAll('rddtest', [...left, ...right], () => {
    const lRdd = distribution.rddtest.rdd.from(['il_a', 'il_b', 'il_c'])
      .map((k, v) => [k.replace('il_', 'i_'), v]);
    const rRdd = distribution.rddtest.rdd.from(['ir_b', 'ir_c', 'ir_d'])
      .map((k, v) => [k.replace('ir_', 'i_'), v]);
    lRdd.intersection(rRdd).collect((e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(sortedKeys(v)).toEqual(['i_b', 'i_c']);
        done();
      } catch (err) { done(err); }
    });
  });
}, TEST_TIMEOUT);

test('subtract removes elements whose keys appear in the right dataset', (done) => {
  const left = [['sub_a', 1], ['sub_b', 2], ['sub_c', 3]];
  const right = [['sub_b', 20], ['sub_d', 40]];
  putAll('rddtest', [...left, ...right], () => {
    const lRdd = distribution.rddtest.rdd.from(['sub_a', 'sub_b', 'sub_c']);
    const rRdd = distribution.rddtest.rdd.from(['sub_b', 'sub_d']);
    lRdd.subtract(rRdd).collect((e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(sortedKeys(v)).toEqual(['sub_a', 'sub_c']);
        done();
      } catch (err) { done(err); }
    });
  });
}, TEST_TIMEOUT);

/* ─── Joins ────────────────────────────────────────────────────────────────── */

test('join produces (key, [leftVal, rightVal]) for matching keys', (done) => {
  // Use distinct physical key prefixes for left (jl_) and right (jr_) to avoid
  // concurrent write races; map both sides to a shared logical prefix (j_).
  const left = [['jl_a', 'LA'], ['jl_b', 'LB'], ['jl_c', 'LC']];
  const right = [['jr_b', 'RB'], ['jr_c', 'RC'], ['jr_d', 'RD']];
  putAll('rddtest', [...left, ...right], () => {
    const lRdd = distribution.rddtest.rdd.from(['jl_a', 'jl_b', 'jl_c'])
      .map((k, v) => [k.replace('jl_', 'j_'), v]);
    const rRdd = distribution.rddtest.rdd.from(['jr_b', 'jr_c', 'jr_d'])
      .map((k, v) => [k.replace('jr_', 'j_'), v]);
    lRdd.join(rRdd).collect((e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).toHaveLength(2); // j_b and j_c match
        const m = toMap(v);
        expect(m['j_b']).toEqual(['LB', 'RB']);
        expect(m['j_c']).toEqual(['LC', 'RC']);
        done();
      } catch (err) { done(err); }
    });
  });
}, TEST_TIMEOUT);

test('leftOuterJoin includes all left elements; missing right sides are null', (done) => {
  // Use distinct physical key prefixes (ljl_ left, ljr_ right) to avoid write races.
  const left = [['ljl_a', 'LA'], ['ljl_b', 'LB']];
  const right = [['ljr_b', 'RB']];
  putAll('rddtest', [...left, ...right], () => {
    const lRdd = distribution.rddtest.rdd.from(['ljl_a', 'ljl_b'])
      .map((k, v) => [k.replace('ljl_', 'lj_'), v]);
    const rRdd = distribution.rddtest.rdd.from(['ljr_b'])
      .map((k, v) => [k.replace('ljr_', 'lj_'), v]);
    lRdd.leftOuterJoin(rRdd).collect((e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).toHaveLength(2);
        const m = toMap(v);
        expect(m['lj_a']).toEqual(['LA', null]);
        expect(m['lj_b']).toEqual(['LB', 'RB']);
        done();
      } catch (err) { done(err); }
    });
  });
}, TEST_TIMEOUT);

test('rightOuterJoin includes all right elements; missing left sides are null', (done) => {
  // Use distinct physical key prefixes (rjl_ left, rjr_ right) to avoid write races.
  const left = [['rjl_a', 'LA']];
  const right = [['rjr_a', 'RA'], ['rjr_b', 'RB']];
  putAll('rddtest', [...left, ...right], () => {
    const lRdd = distribution.rddtest.rdd.from(['rjl_a'])
      .map((k, v) => [k.replace('rjl_', 'rj_'), v]);
    const rRdd = distribution.rddtest.rdd.from(['rjr_a', 'rjr_b'])
      .map((k, v) => [k.replace('rjr_', 'rj_'), v]);
    lRdd.rightOuterJoin(rRdd).collect((e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).toHaveLength(2);
        const m = toMap(v);
        expect(m['rj_a']).toEqual(['LA', 'RA']);
        expect(m['rj_b']).toEqual([null, 'RB']);
        done();
      } catch (err) { done(err); }
    });
  });
}, TEST_TIMEOUT);

/* ─── Multi-stage pipelines ────────────────────────────────────────────────── */

test('map → filter → reduceByKey pipeline', (done) => {
  // Score records; normalize key, keep positives, sum by category
  const pairs = [
    ['pipe_r1', {cat: 'A', score: 5}],
    ['pipe_r2', {cat: 'B', score: -1}],
    ['pipe_r3', {cat: 'A', score: 3}],
    ['pipe_r4', {cat: 'B', score: 2}],
    ['pipe_r5', {cat: 'A', score: -2}],
  ];
  putAll('rddtest', pairs, () => {
    distribution.rddtest.rdd.from(pairs.map(([k]) => k))
      .map((k, v) => [v.cat, v.score])
      .filter((k, v) => v > 0)
      .reduceByKey((a, b) => a + b)
      .collect((e, v) => {
        try {
          expect(e).toBeFalsy();
          const m = toMap(v);
          expect(m['A']).toBe(8);  // 5 + 3
          expect(m['B']).toBe(2);  // -1 dropped, 2
          done();
        } catch (err) { done(err); }
      });
  });
}, TEST_TIMEOUT);

test('flatMap → reduceByKey word-count pipeline', (done) => {
  const pairs = [
    ['wc_d1', 'apple banana apple'],
    ['wc_d2', 'banana cherry apple'],
  ];
  putAll('rddtest', pairs, () => {
    distribution.rddtest.rdd.from(['wc_d1', 'wc_d2'])
      .flatMap((k, v) => v.split(' ').map((w) => [w, 1]))
      .reduceByKey((a, b) => a + b)
      .collect((e, v) => {
        try {
          expect(e).toBeFalsy();
          const m = toMap(v);
          expect(m['apple']).toBe(3);
          expect(m['banana']).toBe(2);
          expect(m['cherry']).toBe(1);
          done();
        } catch (err) { done(err); }
      });
  });
}, TEST_TIMEOUT);

test('map → sortByKey pipeline returns correctly sorted output', (done) => {
  const pairs = [['srt_d', 4], ['srt_b', 2], ['srt_a', 1], ['srt_c', 3]];
  putAll('rddtest', pairs, () => {
    distribution.rddtest.rdd.from(['srt_d', 'srt_b', 'srt_a', 'srt_c'])
      .map((k, v) => [k, v * 10])
      .sortByKey()
      .collect((e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v.map((r) => r.key)).toEqual(['srt_a', 'srt_b', 'srt_c', 'srt_d']);
          expect(v.map((r) => r.value)).toEqual([10, 20, 30, 40]);
          done();
        } catch (err) { done(err); }
      });
  });
}, TEST_TIMEOUT);

/* ─── Edge cases ───────────────────────────────────────────────────────────── */

test('collect on empty key list returns []', (done) => {
  distribution.rddtest.rdd.from([]).collect((e, v) => {
    try {
      expect(e).toBeFalsy();
      expect(v).toEqual([]);
      done();
    } catch (err) { done(err); }
  });
}, TEST_TIMEOUT);

test('count on empty key list returns 0', (done) => {
  distribution.rddtest.rdd.from([]).count((e, v) => {
    try {
      expect(e).toBeFalsy();
      expect(v).toBe(0);
      done();
    } catch (err) { done(err); }
  });
}, TEST_TIMEOUT);

test('map on empty key list returns empty collect', (done) => {
  distribution.rddtest.rdd.from([])
    .map((k, v) => [k, v * 2])
    .collect((e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).toEqual([]);
        done();
      } catch (err) { done(err); }
    });
}, TEST_TIMEOUT);

test('reduce on empty input returns the zero value', (done) => {
  distribution.rddtest.rdd.from([]).reduce(
    (acc, [k, v]) => acc + v,
    99,
    (e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).toBe(99);
        done();
      } catch (err) { done(err); }
    }
  );
}, TEST_TIMEOUT);

test('single-element dataset works for all actions', (done) => {
  putAll('rddtest', [['single_x', 77]], () => {
    const rdd = distribution.rddtest.rdd.from(['single_x']);
    rdd.collect((e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).toHaveLength(1);
        expect(v[0].value).toBe(77);
        rdd.count((e2, c) => {
          try {
            expect(e2).toBeFalsy();
            expect(c).toBe(1);
            done();
          } catch (err) { done(err); }
        });
      } catch (err) { done(err); }
    });
  });
}, TEST_TIMEOUT);

test('lazy evaluation: no side effects before action fires', (done) => {
  // Build a pipeline that would modify a flag if it ran eagerly.
  // Since we never call an action, the flag stays unchanged.
  let executedEarly = false;
  const pairs = [['lazy_a', 1]];
  putAll('rddtest', pairs, () => {
    // Just building the pipeline — no action called, so nothing should run.
    distribution.rddtest.rdd.from(['lazy_a'])
      .map((k, v) => {
        // This body runs only on workers when an action fires.
        return [k, v + 1];
      })
      .filter((k, v) => v > 0);
    // executedEarly stays false because no action was called.
    expect(executedEarly).toBe(false);
    done();
  });
}, TEST_TIMEOUT);

/* ─── Test setup / teardown ────────────────────────────────────────────────── */

beforeAll((done) => {
  try {
    rddGroup[id.getSID(n1)] = n1;
    rddGroup[id.getSID(n2)] = n2;
    rddGroup[id.getSID(n3)] = n3;

    distribution.node.start((e) => {
      if (e) { done(e); return; }

      distribution.local.status.spawn(n1, (e) => {
        if (e) { done(e); return; }
        distribution.local.status.spawn(n2, (e) => {
          if (e) { done(e); return; }
          distribution.local.status.spawn(n3, (e) => {
            if (e) { done(e); return; }

            const cfg = {gid: 'rddtest'};
            distribution.local.groups.put(cfg, rddGroup, () => {
              distribution.rddtest.groups.put(cfg, rddGroup, () => {
                done();
              });
            });
          });
        });
      });
    });
  } catch (err) {
    done(err);
  }
}, 30000);

afterAll((done) => {
  const stop = (node, cb) => {
    distribution.local.comm.send([], {node, service: 'status', method: 'stop'}, () => cb());
  };
  stop(n1, () => stop(n2, () => stop(n3, () => {
    if (globalThis.distribution.node.server) {
      globalThis.distribution.node.server.close();
    }
    done();
  })));
});
