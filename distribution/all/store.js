const id = require('../util/id');
const distribution = global.distribution;

let store = (config) => {
  let context = {};
  context.gid = config.gid || 'all'; // node group
  context.hash = config.hash || id.naiveHash; // hash functio
  return {
    get: (configuration, callback) => {
      if (!configuration) {
        let message = [{key: null, gid: context.gid}];
        distribution[context.gid].comm.send(message,
            {service: 'store', method: 'get'}, (errors, values) => {
              let ketList = Object.values(values).reduce((acc, val) =>
                acc.concat(val), []);
              callback(errors, ketList);
            });
      } else {
        let kid = id.getID(configuration);
        distribution[context.gid].status.get('nid', (e, nids) => {
          // console.log('Get NID: ', e, nids);
          nids = Object.values(nids);
          let nid = context.hash(kid, nids);
          let sid = nid.substring(0, 5);

          distribution.local.groups.get(context.gid, (e, nodes) => {
            let node = nodes[sid];
            let remote = {service: 'store', method: 'get', node: node};
            let message = [{key: configuration, gid: context.gid}];
            distribution.local.comm.send(message, remote, callback);
          });
        });
      }
    },
    put: (object, configuration, callback) => {
      configuration = configuration || id.getID(object);
      let kid = id.getID(configuration);
      distribution[context.gid].status.get('nid', (e, nids) => {
        // console.log('Put NID: ', e, nids);
        nids = Object.values(nids);
        let nid = context.hash(kid, nids);
        let sid = nid.substring(0, 5);

        distribution.local.groups.get(context.gid, (e, nodes) => {
          let node = nodes[sid];
          let remote = {service: 'store', method: 'put', node: node};
          let message = [object, {key: configuration, gid: context.gid}];
          distribution.local.comm.send(message, remote, callback);
        });
      });
    },
    append: (object, configuration, callback) => {
      configuration = configuration || id.getID(object);
      let kid = id.getID(configuration);
      distribution[context.gid].status.get('nid', (e, nids) => {
        // console.log('Append NID: ', e, nids);
        nids = Object.values(nids);
        let nid = context.hash(kid, nids);
        let sid = nid.substring(0, 5);

        distribution.local.groups.get(context.gid, (e, nodes) => {
          let node = nodes[sid];
          let remote = {service: 'store', method: 'append', node: node};
          let message = [object, {key: configuration, gid: context.gid}];
          distribution.local.comm.send(message, remote, callback);
        });
      });
    },
    del: (configuration, callback) => {
      let kid = id.getID(configuration);
      distribution[context.gid].status.get('nid', (e, nids) => {
        nids = Object.values(nids);
        let nid = context.hash(kid, nids);
        let sid = nid.substring(0, 5);

        distribution.local.groups.get(context.gid, (e, nodes) => {
          let node = nodes[sid];
          let remote = {service: 'store', method: 'del', node: node};
          let message = [{key: configuration, gid: context.gid}];
          distribution.local.comm.send(message, remote, callback);
        });
      });
    },
    reconf: (oldGroup, callback) => {
      global.distribution[context.gid].store.get(null, (e, keys) => {
        distribution.local.groups.get(context.gid, (e, newGroup) => {
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
              let remote = {service: 'store', method: 'get'};
              remote.node = oldGroup[oldTargetNode.substring(0, 5)];
              distribution.local.comm.send([keyObj], remote, (e, obj) => {
                remote.method = 'del';
                distribution.local.comm.send([keyObj], remote, (e, v) => {
                  remote.method = 'put';
                  remote.node = newGroup[newTargetNode.substring(0, 5)];
                  distribution.local.comm.send([obj, keyObj], remote, callback);
                });
              });
            }
          }
        });
      });
    },
  };
};

module.exports = store;
