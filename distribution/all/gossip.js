const local = require('../local/local');

let gossip = (config) => {
  let context = {}; // create service-local context.

  context.gid = config.gid || 'all';

  context.subset = config.subset || ((lst) => 3);
  const selectSubsetNodes = function(subsetFunction, nodes) {
    const subsetSize = Math.min(
        subsetFunction(nodes.length),
        Object.keys(nodes).length,
    );
    let selectedNodes = [];

    // Random selection logic based on subsetSize
    while (selectedNodes.length < subsetSize) {
      let index = Math.floor(Math.random() * Object.keys(nodes).length);
      const node = nodes[index];
      if (!selectedNodes.includes(node)) {
        selectedNodes.push(node);
      }
    }

    return selectedNodes;
  };

  return {
    send: (message, remote, callback) => {
      message.timeStamp = Date.now();

      local.groups.get(context.gid, (e, v) => {
        const nodes = Object.keys(v);
        const subset = selectSubsetNodes(context.subset, nodes);

        let counter = 0;
        const errors = {};
        const values = {};

        for (let sid of subset) {
          let node = v[sid];
          local.comm.send(
              [message, remote],
              {node: node, service: 'gossip', method: 'recv'},
              (e, v) => {
                if (e) {
                  errors[sid] = e;
                } else {
                  values[sid] = v;
                }
                counter += 1;
                if (counter === subset.length) {
                  callback(errors, values);
                }
              },
          );
        }
      });
    },
    at: (interval, f, callback) => {
      const funcId = setInterval(f, interval);
      callback(null, funcId.toString());
      return funcId;
    },
    del: (funcId, callback) => {
      clearInterval(parseInt(funcId, 10));
      if (callback) callback();
    },
  };
};

module.exports = gossip;
