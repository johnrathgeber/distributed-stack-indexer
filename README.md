# distribution

This is the distribution library. 

## Environment Setup

We recommend using the prepared [container image](https://github.com/brown-cs1380/container).

## Installation

After you have setup your environment, you can start using the distribution library.
When loaded, distribution introduces functionality supporting the distributed execution of programs. To download it:

```sh
$ npm i '@brown-ds/distribution'
```

This command downloads and installs the distribution library.

## Testing

There are several categories of tests:
  *	Regular Tests (`*.test.js`)
  *	Scenario Tests (`*.scenario.js`)
  *	Extra Credit Tests (`*.extra.test.js`)
  * Student Tests (`*.student.test.js`) - inside `test/test-student`

### Running Tests

By default, all regular tests are run. Use the options below to run different sets of tests:

1. Run all regular tests (default): `$ npm test` or `$ npm test -- -t`
2. Run scenario tests: `$ npm test -- -c` 
3. Run extra credit tests: `$ npm test -- -ec`
4. Run the `non-distribution` tests: `$ npm test -- -nd`
5. Combine options: `$ npm test -- -c -ec -nd -t`

## Usage

To try out the distribution library inside an interactive Node.js session, run:

```sh
$ node
```

Then, load the distribution library:

```js
> let distribution = require("@brown-ds/distribution")();
> distribution.node.start(console.log);
```

Now you have access to the full distribution library. You can start off by serializing some values. 

```js
> s = distribution.util.serialize(1); // '{"type":"number","value":"1"}'
> n = distribution.util.deserialize(s); // 1
```

You can inspect information about the current node (for example its `sid`) by running:

```js
> distribution.local.status.get('sid', console.log); // null 8cf1b (null here is the error value; meaning there is no error)
```

You can also store and retrieve values from the local memory:

```js
> distribution.local.mem.put({name: 'nikos'}, 'key', console.log); // null {name: 'nikos'} (again, null is the error value) 
> distribution.local.mem.get('key', console.log); // null {name: 'nikos'}

> distribution.local.mem.get('wrong-key', console.log); // Error('Key not found') undefined
```

You can also spawn a new node:

```js
> node = { ip: '127.0.0.1', port: 8080 };
> distribution.local.status.spawn(node, console.log);
```

Using the `distribution.all` set of services will allow you to act 
on the full set of nodes created as if they were a single one.

```js
> distribution.all.status.get('sid', console.log); // {} { '8cf1b': '8cf1b', '8cf1c': '8cf1c' } (now, errors are per-node and form an object)
```

You can also send messages to other nodes:

```js
> distribution.local.comm.send(['sid'], {node: node, service: 'status', method: 'get'}, console.log); // null 8cf1c
```

Most methods in the distribution library are asynchronous and take a callback as their last argument.
This callback is called when the method completes, with the first argument being an error (if any) and the second argument being the result.
If you wanted to run this same sequence of commands in a script, you could do something like this (note the nested callbacks):

```js
let distribution = require("@brown-ds/distribution")();
// Now we're only doing a few of the things we did above
const out = (cb) => {
  distribution.local.status.stop(cb); // Shut down the local node
};
distribution.node.start(() => {
  // This will run only after the node has started
  const node = {ip: '127.0.0.1', port: 8765};
  distribution.local.status.spawn(node, (e, v) => {
    if (e) {
      return out(console.log);
    }
    // This will run only after the new node has been spawned
    distribution.all.status.get('sid', (e, v) => {
      // This will run only after we communicated with all nodes and got their sids
      console.log(v); // { '8cf1b': '8cf1b', '8cf1c': '8cf1c' }
      // Shut down the remote node
      distribution.local.comm.send([], {service: 'status', method: 'stop', node: node}, () => {
        // Finally, stop the local node
        out(console.log); // null, {ip: '127.0.0.1', port: 1380}
      });
    });
  });
});
```

# Results and Reflections

# M0: Setup & Centralized Computing

> Add your contact information below and in `package.json`.

* name: `John Rathgeber`

* email: `john_rathgeber@brown.edu`

* cslogin: `jrathgeb`


## Summary

> Summarize your implementation, including the most challenging aspects; remember to update the `report` section of the `package.json` file with the total number of hours it took you to complete M0 (`hours`), the total number of JavaScript lines you added, including tests (`jsloc`), the total number of shell lines you added, including for deployment and testing (`sloc`).


My implementation consists of `6` components addressing T1--8. The most challenging aspect was `process.sh` because `I had to learn how to write .sh files, and had to research the functionality of each suggested command`.


## Correctness & Performance Characterization


> Describe how you characterized the correctness and performance of your implementation.


To characterize correctness, we developed `8 tests` that test the following cases: special 
characters are handled, one term with multiple links are handled, filtering out stopwords is handled, spaces between words in a term are not considered one term, etc..


*Performance*: The throughput of various subsystems is described in the `"throughput"` portion of package.json. The characteristics of my development machines are summarized in the `"dev"` portion of package.json.


## Wild Guess

> How many lines of code do you think it will take to build the fully distributed, scalable version of your search engine? Add that number to the `"dloc"` portion of package.json, and justify your answer below.

I think it will take around 5000 lines of code. My reasoning:
- This milestone took around 500 lines of code
- There are 6 total milestones
- The following milestones will likely involve more lines of code than this one
So on average, maybe I can say that the total amount of lines will be 10x the amount of lines
of code in this milestone.


# M1: Serialization / Deserialization

## Summary

> Summarize your implementation, including key challenges you encountered. Remember to update the `report` section of the `package.json` file with the total number of hours it took you to complete each task of M1 (`hours`) and the lines of code per task.


My implementation comprises `3` software components, totaling `400` lines of code (including tests and scenarios). Key challenges included `ensuring each recursive layer is only stringified once, all types are correctly handled by both the serializer and deserializer, and figuring out how to destructure and label recursive types. For the first challenge, I solved this by abstracting out the recursive part of my serializer, and only called JSON.stringify() once on the final result. For the second challenge, I had to research each type online and figure out what to store for each type (e.g. Errors have name, message, and cause). For the final challenge, I extensively used the reference implementation to gain inspiration on how they serialized recursive types, and drew my own implementation from that`.


## Correctness & Performance Characterization


> Describe how you characterized the correctness and performance of your implementation


*Correctness*: I wrote `8` tests; these tests take `0.648s` to execute. This includes objects with `nested structures, abstract types such as functions, and additional metadata required to reconstruct (e.g. Errors)`.


*Performance*: The latency of various subsystems is described in the `"latency"` portion of package.json. The characteristics of my development machines are summarized in the `"dev"` portion of package.json.

# M2: Actors and Remote Procedure Calls (RPC)

## Summary

> Summarize your implementation, including key challenges you encountered. Remember to update the `report` section of the `package.json` file with the total number of hours it took you to complete each task of M2 (`hours`) and the lines of code per task.


My implementation comprises `3` software components, totaling `490` lines of code. Key challenges included `learning how the http package works, learning exactly what I needed to return for each function, and figuring out how to do counts. For the first problem (http), I had to do a lot of research online about the http server class, and how it is used to send/receive messages. For the second problem (specification), I solved this by analyzing the tests to see what was expected of me for each function. For the third problem (counts), I figured out that we can track counts by having a stored parameter in the distribution.node object, and from there, it was easy to increment as necessary`.


## Correctness & Performance Characterization

> Describe how you characterized the correctness and performance of your implementation


*Correctness*: I wrote `9` tests (including performance tests); these tests take `4s` on average to execute.


*Performance*: I characterized the performance of comm and RPC by sending 1000 service requests in a tight loop. Average throughput and latency is recorded in `package.json`.


## Key Feature

> How would you explain the implementation of `createRPC` to someone who has no background in computer science — i.e., with the minimum jargon possible?

Let's think of nodes as people. `createRPC` allows one person to send instructions to another person, so that other person can ask the original person something specific to the original person and get the correct information back. Let's say I am a node and you are a node. Let's say we want to create an RPC so that you can ask me for the color of my shirt at any time. To do this, I first have to send you instructions on how to ask me (let's say it's some secret code). This is called the stub. Now, when you say the secret code (use the stub), I will respond back to you what color shirt I am wearing, and you can use that data however you please. 

# M3: Node Groups & Gossip Protocols


## Summary

> Summarize your implementation, including key challenges you encountered. Remember to update the `report` section of the `package.json` file with the total number of hours it took you to complete each task of M3 (`hours`) and the lines of code per task.


My implementation comprises `7` new software components, totaling `580` added lines of code over the previous implementation. Key challenges included `understanding how groups and group views work, figuring out the difference between distributed and local services/methods, and aggregation. For the first challenge, I had to watch the gearup a second time to fully understand the schema while working through the scenarios. Doing that while writing my own tests at the same time really helped me understand what was really going on. For the second challenge, as soon as I figured out how to implement all.comm.send (which took a lot of trial and error), I was able to figure out how the other distributed services worked, because they were very similar to all.comm.send. For the third challlenge, I initially thought we should aggregate both heapUsed and heapTotal, but after painstakingly debugging the tests on Gradescope (locally my tests passed but not on Gradescope), I realized that we should only aggregate heapTotal, as heapUsed is calculated on a node-by-node basis`.


## Correctness & Performance Characterization

> Describe how you characterized the correctness and performance of your implementation


*Correctness* -- number of tests and time they take. I wrote 7 tests total, and they take roughly 25 seconds to run locally (due to the performance tests).


*Performance* -- spawn times (all students) and gossip (lab/ec-only). I wrote tests to measure the throughput and latency of spawn(). These tests spawn 100 different nodes each to accurately measure the average latency and throughput.


## Key Feature

> What is the point of having a gossip protocol? Why doesn't a node just send the message to _all_ other nodes in its group?

`If we made one node send the message to all nodes right away, we would have a super heavy load of network traffic all at once. By using the gossip protocol, we distribute this load across a (short) period of time, while still allowing the message to reach all other nodes reliably.`

# M4: Distributed Storage


## Summary

> Summarize your implementation, including key challenges you encountered

`My implementation comprises 3 new software components (mem, store, and hashing), totaling 700 added lines of code over the previous implementation. Key challenges included constructing the distributed student tests, implementing consistent hashing, and resolving file issues for store. For the first problem, I ran into issues with the beforeAll() and afterAll() functions for some reason (which I copied from the given distributed store tests), so I had to modify them to accommodate only two nodes running and avoid exiting the program early. For the second problem, I was running into trouble with the sorting of the bigint list, and I researched how to sort it and I found out that it sorts lexicographically, so I found a workaround to that. For the third problem, I kept getting 'file not found' errors with the distributed store, and it took me a while to figure out that the problem was actually with the hashing functions, and not my implementation of store.`


Remember to update the `report` section of the `package.json` file with the total number of hours it took you to complete each task of M4 (`hours`) and the lines of code per task.


## Correctness & Performance Characterization

> Describe how you characterized the correctness and performance of your implementation


*Correctness* -- number of tests and time they take. I wrote 5 tests, and they take roughly 0.8s to run (not including the performance tests).


*Performance* -- insertion and retrieval. I wrote one performance test, which takes about 3s to run. It measures the throughput and latency, while sending 1000 gets and puts.


## Key Feature

> Why is the `reconf` method designed to first identify all the keys to be relocated and then relocate individual objects instead of fetching all the objects immediately and then pushing them to their corresponding locations?

Doing everything all at once would cause a very heavy load on the system, because if the store is very large, then the system has to handle a massive batch request all at once, which could overload it. Instead, by doing a streaming approach, the system takes an amount which it can handle and continously relocates keys, ensuring that it completes its job without error and still in a timely manner.

# M5: Distributed Execution Engine


## Summary

> Summarize your implementation, including key challenges you encountered. Remember to update the `report` section of the `package.json` file with the total number of hours it took you to complete each task of M5 (`hours`) and the lines of code per task.


My implementation comprises `<number>` new software components, totaling `<number>` added lines of code over the previous implementation. Key challenges included `<1, 2, 3 + how you solved them>`.


## Correctness & Performance Characterization

> Describe how you characterized the correctness and performance of your implementation


*Correctness*: I wrote <X> cases testing <1, 2, 3>.


*Performance*: My <workflow> can sustain <throughput> <unit>/second, with an average latency of <number> seconds per <unit>.


## Key Feature

> Which extra features did you implement and how?
