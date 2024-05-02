const id = require('../util/id');
const serialization = require('../util/serialization');
const {spawn} = require('node:child_process');
const wire = require('../util/wire');
const path = require('path');

const node = global.nodeConfig;

global.states = {
  counts: 0,
  servicesMap: new Map(),
};
global.toLocal = new Map();

const status = {};

status.get = function(key, callback) {
  callback = callback || function() {};
  if (key === 'sid') {
    callback(null, id.getSID(node));
  } else if (key === 'nid') {
    callback(null, id.getNID(node));
  } else if (key === 'ip') {
    callback(null, node.ip);
  } else if (key === 'port') {
    callback(null, node.port);
  } else if (key === 'counts') {
    callback(null, global.states.counts);
  } else if (key === 'heapTotal') {
    callback(null, process.memoryUsage().heapTotal);
  } else if (key === 'heapUsed') {
    callback(null, process.memoryUsage().heapUsed);
  } else {
    callback(new Error('Invalid Key'), null);
  }
};

status.stop = function(callback) {
  callback(null, node);
  console.log('Node Stop ', node);
  process.exit(0);
};

status.spawn = function(conf, callback) {
  conf.onStart = conf.onStart || function() {};
  let f = `
    let onStart = ${conf.onStart.toString()};
    let rpcObject = ${wire.createRPC(wire.toAsync(callback)).toString()};
    onStart();
    rpcObject(null, global.nodeConfig, function() {});
  `;
  conf.onStart = new Function(f);
  const cmdPath = path.join(__dirname, '../../distribution.js');
  const serializeObj = serialization.serialize(conf);
  spawn('node', [cmdPath, '--config', `${serializeObj}`], {detached: true, stdio: 'inherit'});
};

module.exports = status;
