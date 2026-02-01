/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Imporant: Do not modify any of the test headers (i.e., the test('header', ...) part). Doing so will result in grading penalties.
*/

const distribution = require('../../distribution.js')();
require('../helpers/sync-guard');

test('(1 pts) student test', () => {
  const obj_1 = {a: ["a"], b: 3};
  const obj_2 = {"a": ["a"], "b": 3};
  const obj_3 = {"a": ["a"], "b": "3"};
  const so1 = distribution.util.serialize(obj_1);
  const so2 = distribution.util.serialize(obj_2);
  const so3 = distribution.util.serialize(obj_3);
  expect(so1).toEqual(so2);
  expect(so1).not.toEqual(so3);
  expect(so1).toEqual('{"type":"object","value":{"a":{"type":"array","value":{"0":{"type":"string","value":"a"}}},"b":{"type":"number","value":"3"}}}');
  expect(so3).toEqual('{"type":"object","value":{"a":{"type":"array","value":{"0":{"type":"string","value":"a"}}},"b":{"type":"string","value":"3"}}}');
  expect(distribution.util.deserialize(so1) == obj_1);
  expect(distribution.util.deserialize(so2) == obj_2);
  expect(distribution.util.deserialize(so3) == obj_3);
  expect(distribution.util.deserialize(so2) != distribution.util.deserialize(so3));
});


test('(1 pts) student test', () => {
  const obj_1 = ["1"];
  const obj_2 = {a: ["1"]};
  const obj_3 = [{a:"1"}];
  const so1 = distribution.util.serialize(obj_1);
  const so2 = distribution.util.serialize(obj_2);
  const so3 = distribution.util.serialize(obj_3);
  expect(so1).not.toEqual(so2);
  expect(so1).not.toEqual(so3);
  expect(so2).not.toEqual(so3);
  expect(so1).toEqual('{"type":"array","value":{"0":{"type":"string","value":"1"}}}');
  expect(so2).toEqual('{"type":"object","value":{"a":{"type":"array","value":{"0":{"type":"string","value":"1"}}}}}');
  expect(so3).toEqual('{"type":"array","value":{"0":{"type":"object","value":{"a":{"type":"string","value":"1"}}}}}');
  expect(distribution.util.deserialize(so1) == obj_1);
  expect(distribution.util.deserialize(so2) == obj_2);
  expect(distribution.util.deserialize(so3) == obj_3);
  expect(distribution.util.deserialize(so1) != distribution.util.deserialize(so3));
  expect(distribution.util.deserialize(so2) != distribution.util.deserialize(so3));
  expect(distribution.util.deserialize(so1) != distribution.util.deserialize(so2));
});


test('(1 pts) student test', () => {
  const object = {a: [3], b: new Date("2023-12-25T10:00:00"), c: new Error("Something went wrong!"), e: true, g: null, h: 3, i: {a: "hi"}, j: "s", k: undefined};
  const so = distribution.util.serialize(object);
  expect(so).toEqual('{"type":"object","value":{"a":{"type":"array","value":{"0":{"type":"number","value":"3"}}},"b":{"type":"date","value":"2023-12-25T10:00:00.000Z"},"c":{"type":"error","value":{"type":"object","value":{"name":{"type":"string","value":"Error"},"message":{"type":"string","value":"Something went wrong!"},"cause":{"type":"undefined","value":""}}}},"e":{"type":"boolean","value":"true"},"g":{"type":"null","value":""},"h":{"type":"number","value":"3"},"i":{"type":"object","value":{"a":{"type":"string","value":"hi"}}},"j":{"type":"string","value":"s"},"k":{"type":"undefined","value":""}}}');
  expect(distribution.util.deserialize(so)).toEqual(object);
});

test('(1 pts) student test', () => {
  const obj_1 = 3;
  const obj_2 = "3";
  const obj_3 = "three";
  const obj_4 = [3]
  const obj_5 = ["3"]
  const so1 = distribution.util.serialize(obj_1);
  const so2 = distribution.util.serialize(obj_2);
  const so3 = distribution.util.serialize(obj_3);
  const so4 = distribution.util.serialize(obj_4);
  const so5 = distribution.util.serialize(obj_5);
  expect(so1).not.toEqual(so2);
  expect(so1).not.toEqual(so3);
  expect(so1).not.toEqual(so4);
  expect(so1).not.toEqual(so5);
  expect(so2).not.toEqual(so3);
  expect(so2).not.toEqual(so4);
  expect(so2).not.toEqual(so5);
  expect(so3).not.toEqual(so4);
  expect(so3).not.toEqual(so5);
  expect(so4).not.toEqual(so5);
  expect(so1).toEqual('{"type":"number","value":"3"}');
  expect(so2).toEqual('{"type":"string","value":"3"}');
  expect(so3).toEqual('{"type":"string","value":"three"}');
  expect(so4).toEqual('{"type":"array","value":{"0":{"type":"number","value":"3"}}}');
  expect(so5).toEqual('{"type":"array","value":{"0":{"type":"string","value":"3"}}}');
  expect(distribution.util.deserialize(so1) == obj_1);
  expect(distribution.util.deserialize(so2) == obj_2);
  expect(distribution.util.deserialize(so3) == obj_3);
  expect(distribution.util.deserialize(so4) == obj_4);
  expect(distribution.util.deserialize(so5) == obj_5);
  const dso1 = distribution.util.deserialize(so1);
  const dso2 = distribution.util.deserialize(so2);
  const dso3 = distribution.util.deserialize(so3);
  const dso4 = distribution.util.deserialize(so4);
  const dso5 = distribution.util.deserialize(so5);
  expect(dso1).not.toEqual(dso2);
  expect(dso1).not.toEqual(dso3);
  expect(dso1).not.toEqual(dso4);
  expect(dso1).not.toEqual(dso5);
  expect(dso2).not.toEqual(dso3);
  expect(dso2).not.toEqual(dso4);
  expect(dso2).not.toEqual(dso5);
  expect(dso3).not.toEqual(dso4);
  expect(dso3).not.toEqual(dso5);
  expect(dso4).not.toEqual(dso5);
});

test('(1 pts) student test', () => {
  const s = '{"type":"number","value":"3"}';
  const so = distribution.util.serialize(s);
  expect(so).toEqual('{"type":"string","value":"{\\\"type\\\":\\\"number\\\",\\\"value\\\":\\\"3\\\"}"}');
  const dso = distribution.util.deserialize(so);
  expect(dso).toEqual('{"type":"number","value":"3"}');
  const ddso = distribution.util.deserialize(dso);
  expect(ddso).toEqual(3);
});

test('extra 1', () => {
  const f = function(a, b) {return a;};
  const fo = distribution.util.serialize(f);
  expect(fo).toEqual(`{"type":"function","value":"function (a, b) {\\n    return a;\\n  }\"}`);
  const dfo = distribution.util.deserialize(fo);
  expect(typeof dfo).toEqual("function");
  expect(dfo(3, 4)).toEqual(3);
})
