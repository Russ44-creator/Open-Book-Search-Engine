let routes = (config) => {
  let context = {};
  context.gid = config.gid || 'all';
  return {
    put: (service, serviceName, callback) => {
      callback = callback || function() {};
      global.distribution[context.gid].comm.send(
          [service, serviceName], {service: 'routes', method: 'put'}, callback);
    },
  };
};

module.exports = routes;
