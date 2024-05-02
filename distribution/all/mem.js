let local = require('../local/local');
const distribution = global.distribution;
const id = require('../util/id');

let mem = (config) => {
  let context = {};
  context.gid = config.gid || 'all';
  context.hash = config.hash || id.naiveHash;
  return {
    get: (configuration, callback) => {
      if (!configuration) {
        let message = [{key: null, gid: context.gid}];
        distribution[context.gid].comm.send(message,
            {service: 'mem', method: 'get'}, (errors, values) => {
              let keyList = Object.values(values).reduce((acc, val) =>
                acc.concat(val), []);
              callback(errors, keyList);
            });
      } else {
        let kid = id.getID(configuration);
        distribution[context.gid].status.get('nid', (e, nids) => {
          nids = Object.values(nids);
          let nid = context.hash(kid, nids);
          let sid = nid.substring(0, 5);

          local.groups.get(context.gid, (e, nodes) => {
            let node = nodes[sid];
            let remote = {service: 'mem', method: 'get', node: node};
            let message = [{key: configuration, gid: context.gid}];
            local.comm.send(message, remote, callback);
          });
        });
      }
    },
    put: (object, configuration, callback) => {
      configuration = configuration || id.getID(object);
      let kid = id.getID(configuration);
      distribution[context.gid].status.get('nid', (e, nids) => {
        nids = Object.values(nids);
        let nid = context.hash(kid, nids);
        let sid = nid.substring(0, 5);

        local.groups.get(context.gid, (e, nodes) => {
          let node = nodes[sid];
          let remote = {service: 'mem', method: 'put', node: node};
          let message = [object, {key: configuration, gid: context.gid}];
          local.comm.send(message, remote, callback);
        });
      });
    },
    append: (object, configuration, callback) => {
      configuration = configuration || id.getID(object);
      let kid = id.getID(configuration);
      distribution[context.gid].status.get('nid', (e, nids) => {
        nids = Object.values(nids);
        let nid = context.hash(kid, nids);
        let sid = nid.substring(0, 5);

        local.groups.get(context.gid, (e, nodes) => {
          let node = nodes[sid];
          let remote = {service: 'mem', method: 'append', node: node};
          let message = [object, {key: configuration, gid: context.gid}];
          local.comm.send(message, remote, callback);
        });
      });
    },
    del: (configuration, callback) => {
      let kid = id.getID(configuration);
      distribution[context.gid].status.get('nid', (e, nids) => {
        nids = Object.values(nids);
        let nid = context.hash(kid, nids);
        let sid = nid.substring(0, 5);

        local.groups.get(context.gid, (e, nodes) => {
          let node = nodes[sid];
          let remote = {service: 'mem', method: 'del', node: node};
          let message = [{key: configuration, gid: context.gid}];
          local.comm.send(message, remote, callback);
        });
      });
    },
    reconf: (oldGroup, callback) => {
      global.distribution[context.gid].mem.get(null, (e, keys) => {
        local.groups.get(context.gid, (e, newGroup) => {
          let oldAllNodeInformation = Object.values(oldGroup);
          let oldNids = oldAllNodeInformation.map((node) => {
            return id.getNID(node);
          });
          let newAllNodeInformation = Object.values(newGroup);
          let newNids = newAllNodeInformation.map((node) => {
            return id.getNID(node);
          });

          for (let key of keys) {
            const kid = id.getID(key);
            const oldTargetNode = context.hash(kid, oldNids);
            const newTargetNode = context.hash(kid, newNids);
            const keyObj = {key: key, gid: context.gid};

            // If two hashes are different, we need to reconf that key
            if (oldTargetNode !== newTargetNode) {
              let remote = {service: 'mem', method: 'get'};
              remote.node = oldGroup[oldTargetNode.substring(0, 5)];
              local.comm.send([keyObj], remote, (e, obj) => {
                remote.method = 'del';
                local.comm.send([keyObj], remote, (e, v) => {
                  remote.method = 'put';
                  remote.node = newGroup[newTargetNode.substring(0, 5)];
                  local.comm.send([obj, keyObj], remote, callback);
                });
              });
            }
          }
        });
      });
    },
  };
};

module.exports = mem;
