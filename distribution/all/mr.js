const id = require('../util/id');


const mr = function(config) {
  let context = {};
  context.gid = config.gid || 'all';
  context.hash = config.hash || id.naiveHash;

  return {
    exec: (configuration, callback) => {
      /* Change this with your own exciting Map Reduce code! */
      const {keys, map, reduce, compact, storeReducedValue} = configuration;

      // Setup phase
      const mrService = {};

      mrService.mapper = (keys, map, compact, gid, callback) => {
        console.log('mapper get called');
        let allRes = [];
        const storeFindConfig = {key: null, gid: gid};
        global.distribution.local.store.get(storeFindConfig, (e, v) => {
          let matchedKeys = [];
          // console.log('V matchedKeys: ', v);
          // console.log('Keys matchedKeys: ', keys);
          for (let i = 0; i < v.length; i++) {
            let key = v[i];
            if (keys.includes(key)) {
              matchedKeys.push(key);
            }
          }
          let count = 0;
          // console.log('Mapper matchedKeys: ', matchedKeys);
          if (count === matchedKeys.length) {
            callback(e, allRes);
          }
          for (let key of matchedKeys) {
            const singleConfig = {key: key, gid: gid};
            global.distribution.local.store.get(singleConfig,
                async (e, value) => {
                  if (e != null) {
                    callback(new Error('Local Store Get Error'), null);
                  }
                  // console.log('Map andand Value: ', key, value);
                  let res = await map(key, value);
                  if (compact != null) {
                    res = compact(res);
                  } else {
                    // console.log('Compact is null');
                  }
                  // console.log('Map res: ', key, value, Array.isArray(res), res);
                  if (Array.isArray(res) && res.length == 0) {
                    const deleteConfig = {key: key, gid: gid};
                    global.distribution.local.store.del(deleteConfig, (e, v) => {
                      count++;
                      if (count === matchedKeys.length) {
                        callback(e, allRes);
                      }
                    });
                  } else {
                    let putConfig = {key: key+'_res', gid: gid};
                    global.distribution.local.store.put(res,
                        putConfig, (e, v) => {
                        // delete original store: '000'
                          const deleteConfig = {key: key, gid: gid};
                          global.distribution.local.store.del(deleteConfig, (e, v) => {
                            count++;
                            if (count === matchedKeys.length) {
                              callback(e, allRes);
                            }
                          });
                        });
                  }
                });
          }
        });
      };

      mrService.reducer = (keys, reduce, gid, storeReducedValue, callback) => {
        const storeFindConfig = {key: null, gid: gid};
        // console.log('keys in reducer: ', keys);
        global.distribution.local.store.get(storeFindConfig, (e, v) => {
          let matchedKeys = [];
          for (let i = 0; i < v.length; i++) {
            let key = v[i];
            if (keys.includes(key)) {
              matchedKeys.push(key);
            }
          }
          let count = 0;
          if (count === matchedKeys.length) {
            callback(e, []);
          }
          let allRes = [];
          for (let key of matchedKeys) {
            const singleConfig = {key: key, gid: gid};
            global.distribution.local.store.get(singleConfig, (e, value) => {
              if (e != null) {
                callback(new Error('Local Store Get Error'), null);
              }

              let res = reduce(key, value);
              allRes.push(res);

              if (!storeReducedValue) {
                count++;
                if (count === matchedKeys.length) {
                  callback(e, allRes);
                }
              } else {
                let putConfig = {key: key, gid: gid};
                const deleteConfig = {key: key, gid: gid};
                global.distribution.local.store.del(deleteConfig, (e, v) => {
                  global.distribution.local.store.put(res,
                      putConfig, (e, v) => {
                        count++;
                        if (count === matchedKeys.length) {
                          callback(e, allRes);
                        }
                      });
                });
              };
            });
          }
        });
      };

      mrService.shuffle = (keys, gid, callback) => {
        const storeFindConfig = {key: null, gid: gid};
        keys = keys.map((item) => `${item}_res`);
        console.log('Shuffle Keys: ', keys);

        global.distribution.local.store.get(storeFindConfig, (e, v) => {
          let matchedKeys = [];
          for (let i = 0; i < v.length; i++) {
            let key = v[i];
            if (keys.includes(key)) {
              matchedKeys.push(key);
            }
          }
          let count = 0;
          // let groupCount = 0;
          // let expectedCount = 0;
          let allKeys = [];
          if (matchedKeys.length === 0) {
            callback(e, allKeys);
          }
          // console.log('Shuffle Matched Keys: ', matchedKeys);

          global.distribution[gid].status.get('nid', (e, nids) => {
            // console.log('Shuffle nids: ', nids);
            let nidList = Object.values(nids);
            const nodeSendList = {};
            for (let nid of nidList) {
              let sid = nid.substring(0, 5);
              nodeSendList[sid] = {};
            }
            distribution.local.groups.get(gid, (e, group) => {
              let sendToNodes = (nodeSendList, allKeys, callback) => {
                let count = 0;
                let keys = Object.keys(nodeSendList); // Get an array of keys

                for (let sid of keys) {
                  let node = group[sid];
                  let remote = {
                    node: node,
                    service: 'store',
                    method: 'appendAll',
                  };
                  global.distribution.local.comm.send([nodeSendList[sid], {gid: gid}], remote, () => {
                    count++;
                    if (count === keys.length) {
                      callback(e, allKeys);
                    }
                  });
                }
              };

              for (let key of matchedKeys) {
                const singleConfig = {key: key, gid: gid};
                global.distribution.local.store.get(singleConfig, (e, value) => {
                  global.distribution.local.store.del(singleConfig, () => {
                    if (e != null) {
                      callback(new Error('Local Store Get Error'), null);
                    } else if (Array.isArray(value)) {
                      // console.log('Shuffle value: ', value);
                      let countLevel2 = 0;
                      let expectedCount = value.length;
                      for (let obj of value) {
                        let keyList = Object.keys(obj);
                        const k = keyList[0];
                        allKeys.push(k);
                        const valueShuffle = obj[keyList[0]];
                        let kid = global.distribution.util.id.getID(k);
                        let nodeSID = global.config.hash(kid, nidList).substring(0, 5);
                        nodeSendList[nodeSID][k] = nodeSendList[nodeSID][k] || [];
                        nodeSendList[nodeSID][k].push(valueShuffle);
                        countLevel2++;
                        if (countLevel2 === expectedCount) {
                          count++;
                          if (count === matchedKeys.length) {
                            sendToNodes(nodeSendList, allKeys, callback);
                          }
                        }
                      }
                    } else {
                      let keyList = Object.keys(value);
                      const k = keyList[0];
                      if (k) {
                        allKeys.push(k);
                        const valueShuffle = value[keyList[0]];
                        let kid = global.distribution.util.id.getID(k);
                        let nodeSID = global.config.hash(kid, nidList).substring(0, 5);
                        nodeSendList[nodeSID][k] = nodeSendList[nodeSID][k] || [];
                        nodeSendList[nodeSID][k].push(valueShuffle);
                        count++;
                        if (count === matchedKeys.length) {
                          sendToNodes(nodeSendList, allKeys, callback);
                        }
                      } else {
                        count++;
                        if (count === matchedKeys.length) {
                          sendToNodes(nodeSendList, allKeys, callback);
                        }
                      }
                    }
                  });
                });
              }
            });
          });
        });
      };

      const mrServiceName = 'mr_' + id.getID(mrService);

      global.distribution[context.gid].routes.put(mrService,
          mrServiceName, (e, v) => {
            // console.log('Finish Setup!', e, v);
            let remote = {
              service: mrServiceName,
              method: 'mapper',
            };
            let message = [keys, map, compact, context.gid];
            global.distribution[context.gid].comm.send(message,
                remote, (e, v) => {
                  // console.log('Finish Map!', e, v);
                  let message = [keys, context.gid];
                  let remote = {
                    service: mrServiceName,
                    method: 'shuffle',
                  };
                  global.distribution[context.gid].comm.send(message,
                      remote, (e, keyMap) => {
                        // console.log('Get shuffle value: ', keyMap);
                        let keySet = new Set();
                        for (let keys of Object.values(keyMap)) {
                          for (let key of keys) {
                            keySet.add(key);
                          }
                        }
                        // console.log('KeySet: ', keySet);
                        let message = [[...keySet], reduce, context.gid, storeReducedValue];
                        let remote = {
                          service: mrServiceName,
                          method: 'reducer',
                        };
                        global.distribution[context.gid].comm.send(message,
                            remote, (e, res) => {
                              let finalRes = [];
                              for (let resList of Object.values(res)) {
                                for (let r of resList) {
                                  if (r) {
                                    finalRes.push(r);
                                  }
                                }
                              }
                              // console.log('Final res: ', finalRes);
                              callback(null, finalRes);
                            });
                      });
                });
          });
    },
  };
};

module.exports = mr;
