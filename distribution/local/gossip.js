const comm = require('./comm');

const gossip = {};
let receivedMessages = new Set();

gossip.recv = (message, remote, callback) => {
  const messageId = `${message.id}-${message.timestamp}`;
  if (receivedMessages.has(messageId)) {
    return;
  }
  receivedMessages.add(messageId);
  comm.send(message, {node: global.nodeConfig, ...remote}, (e, v) => {
    if (callback) {
      callback(e, v);
    }
  });
  global.distribution[message[0]].gossip.send(message, remote);
};

module.exports = gossip;
