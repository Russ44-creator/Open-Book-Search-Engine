const http = require('http');
const serialization = require('../util/serialization');

const comm = {};

comm.send = (message, remote, callback) => {
  const serializedMessage = serialization.serialize(message);
  const messageLength = Buffer.byteLength(serializedMessage);
  const options = {
    hostname: remote.node.ip,
    port: remote.node.port,
    path: `/${remote.service}/${remote.method}`,
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
      'Content-Length': messageLength,
    },
  };

  const req = http.request(options, (func) => {
    let data = '';

    func.on('data', (chunk) => {
      data += chunk;
    });

    func.on('end', () => {
      callback(...serialization.deserialize(data));
    });
  });

  req.on('error', (error) => {
    callback(new Error(error), null);
  });

  req.write(serializedMessage);
  req.end();
};

module.exports = comm;
