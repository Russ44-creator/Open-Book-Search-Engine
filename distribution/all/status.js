const localStatus = require('../local/status');
const localGroups = require('../local/groups');
const id = require('../util/id');

let status = (config) => {
  global.config = config;
  global.config.hash = config.hash || id.naiveHash;
  let context = {};
  context.gid = config.gid || 'all'; // contains a property named gid
  return {
    get: (key, callback) => {
      let remote = {service: 'status', method: 'get'};
      let message = [
        key, // configuration
      ];
      global.distribution[context.gid].comm.send(message,
          remote, (errors, values) => {
            if (key === 'heapTotal') {
              let sum = 0;
              for (let key of Object.keys(values)) {
                sum += values[key];
              }
              callback(errors, sum);
            } else {
              callback(errors, values);
            }
          });
    },
    stop: (callback) => {
      let remote = {service: 'status', method: 'stop'};
      let message = [];
      global.distribution[context.gid].comm.send(message, remote, callback);
      // process.exit(0);
    },
    spawn: (conf, callback) => {
      let remote = {service: 'groups', method: 'add'};
      localStatus.spawn(conf, (e, v) => {
        if (e) {
          callback(e);
        } else {
          localGroups.add(context.gid, conf, ()=>{});
          callback(null, v);
        }
        let message = [
          context.gid,
          conf,
        ];
        global.distribution[context.gid].comm.send(message, remote, ()=>{});
      });
    },
  };
};

module.exports = status;
