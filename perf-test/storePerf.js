const async = require('async');
const distribution = require('../distribution');
const id = distribution.util.id;
const groupsTemplate = require('../distribution/all/groups');
const storePerfGroup = {};

let localServer = null;
global.nodeConfig = {ip: '127.0.0.1', port: 7070};

const n1 = {ip: '127.0.0.1', port: 7110};
const n2 = {ip: '127.0.0.1', port: 7111};
const n3 = {ip: '127.0.0.1', port: 7112};
const n4 = {ip: '127.0.0.1', port: 7113};
const n5 = {ip: '127.0.0.1', port: 7114};

storePerfGroup[id.getSID(n1)] = n1;
storePerfGroup[id.getSID(n2)] = n2;
storePerfGroup[id.getSID(n3)] = n3;
storePerfGroup[id.getSID(n4)] = n4;
storePerfGroup[id.getSID(n5)] = n5;

const startNodes = (cb) => {
  distribution.local.status.spawn(n1, (e, v) => {
    distribution.local.status.spawn(n2, (e, v) => {
      distribution.local.status.spawn(n3, (e, v) => {
        distribution.local.status.spawn(n4, (e, v) => {
          distribution.local.status.spawn(n5, (e, v) => {
            cb();
          });
        });
      });
    });
  });
};


const generateDataset = (size) => {
  let dataset = [];
  for (let i = 0; i < size; i++) {
    dataset.push({
      [`key${i}`]: {
        property1: 'property1',
        property1: 'property2',
        property1: 'property3',
        property1: 'property4',
        property5: 'property5'}});
  }
  return dataset;
};

const OBJSIZE = 100000; // We decided to try 100, 500, 1000, 5000, 10000, 50000, 100000, 200000
const CURRENTLIMIT = 10;
let dataset = generateDataset(OBJSIZE);

const terminate = () => {
  console.log('-------------NODES CLEANING----------');
  let remote = {service: 'status', method: 'stop'};
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      localServer.close();
    });
  });
};


distribution.node.start((server) => {
  localServer = server;
  const storePerfConfig = {gid: 'test', hash: id.rendezvousHash};
  startNodes(() => {
    groupsTemplate(storePerfConfig).put(storePerfConfig, storePerfGroup, (e, v) => {
      console.log('Put nodes into group: ', e, v);
      let startTime = Date.now();

      // Use async.eachLimit to control the number of concurrent operations
      async.eachLimit(dataset, CURRENTLIMIT, (o, callback) => {
        let key = Object.keys(o)[0];
        let value = o[key];
        distribution.test.store.put(value, key, (err) => {
          if (err) {
            callback(err);
          } else {
            callback();
          }
        });
      }, (err) => {
        if (err) {
          console.error('Error during PUT operations:', err);
        } else {
          let endTime = Date.now();
          let duration = (endTime - startTime) / 1000;
          let putThroughput = dataset.length / duration;
          console.log(`Put Duration: ${duration.toFixed(3)} secs`);
          console.log(`Put Throughput: ${putThroughput.toFixed(3)} ops/sec`);

          startTime = Date.now();
          async.eachLimit(dataset, CURRENTLIMIT, (o, callback) => {
            let key = Object.keys(o)[0];
            distribution.test.store.get(key, (err) => {
              if (err) {
                callback(err);
              } else {
                callback();
              }
            });
          }, (err) => {
            if (err) {
              console.error('Error during GET operations:', err);
            } else {
              endTime = Date.now();
              duration = (endTime - startTime) / 1000;
              let getThroughput = dataset.length / duration;
              console.log(`Get Duration: ${duration.toFixed(2)} secs`);
              console.log(`Get Throughput: ${getThroughput.toFixed(2)} ops/sec`);
              terminate();
            }
          });
        }
      });
    });
  });
});


/*
This won't work when objsize > ~2000.
Will encounter the EADDRNOTAVAIL error
Solution: using async.eachLimit to set a limit on the number of concurrent operations
*/
// distribution.node.start((server) => {
//   localServer = server;
//   const storePerfConfig = {gid: 'test'};
//   startNodes(() => {
//     groupsTemplate(storePerfConfig).put(storePerfConfig, storePerfGroup, (e, v) => {
//       console.log('Put nodes into group: ', e, v);
//       let cnt = 0;
//       let startTime = Date.now(); // Start timing here

//       dataset.forEach((o) => {
//         let key = Object.keys(o)[0];
//         let value = o[key];
//         distribution.test.store.put(value, key, (e, v) => {
//           if (!e) {
//             cnt++;
//             if (cnt === dataset.length) {
//               let endTime = Date.now(); // End timing here
//               let duration = (endTime - startTime) / 1000; // Convert ms to seconds
//               let putThroughput = OBJSIZE / duration; // Calculate throughput
//               console.log(`Put Throughput: ${putThroughput.toFixed(2)} ops/sec`);

//               cnt = 0;
//               startTime = Date.now(); // Start timing for GET test
//               dataset.forEach((o) => {
//                 let key = Object.keys(o)[0];
//                 distribution.test.store.get(key, (e, v) => {
//                   cnt++;
//                   if (cnt === dataset.length) {
//                     endTime = Date.now(); // End timing for GET test
//                     let duration = (endTime - startTime) / 1000; // Convert ms to seconds
//                     let getThroughput = OBJSIZE / duration; // Calculate throughput
//                     console.log(`Get Throughput: ${getThroughput.toFixed(2)} ops/sec`);
//                     terminate();
//                   }
//                 });
//               });
//             }
//           }
//         });
//       });
//     });
//   });
// });
