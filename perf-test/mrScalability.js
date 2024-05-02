const async = require('async');
const distribution = require('../distribution');
const id = distribution.util.id;
const groupsTemplate = require('../distribution/all/groups');
const mrGroup = {};

let localServer = null;
global.nodeConfig = {ip: '127.0.0.1', port: 7070};

const n1 = {ip: '127.0.0.1', port: 7110};
const n2 = {ip: '127.0.0.1', port: 7111};
const n3 = {ip: '127.0.0.1', port: 7112};
const n4 = {ip: '127.0.0.1', port: 7113};
const n5 = {ip: '127.0.0.1', port: 7114};
const n6 = {ip: '127.0.0.1', port: 7114};
const n7 = {ip: '127.0.0.1', port: 7114};
const n8 = {ip: '127.0.0.1', port: 7114};

mrGroup[id.getSID(n1)] = n1;
mrGroup[id.getSID(n2)] = n2;
mrGroup[id.getSID(n3)] = n3;
mrGroup[id.getSID(n4)] = n4;
mrGroup[id.getSID(n5)] = n5;
mrGroup[id.getSID(n6)] = n6;
mrGroup[id.getSID(n7)] = n7;
mrGroup[id.getSID(n8)] = n8;

const startNodes = (cb) => {
  distribution.local.status.spawn(n1, (e, v) => {
    distribution.local.status.spawn(n2, (e, v) => {
      distribution.local.status.spawn(n3, (e, v) => {
        distribution.local.status.spawn(n4, (e, v) => {
          distribution.local.status.spawn(n5, (e, v) => {
            distribution.local.status.spawn(n6, (e, v) => {
              distribution.local.status.spawn(n7, (e, v) => {
                distribution.local.status.spawn(n8, (e, v) => {
                  cb();
                });
              });
            });
          });
        });
      });
    });
  });
};


// const OBJSIZE = 100000; // We decided to try 100, 500, 1000, 5000, 10000, 50000, 100000, 200000
const CURRENTLIMIT = 10;

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

const mapFun = (key, value) => {
  // map each word to a key-value pair like {word: 1}
  let words = value.split(/(\s+)/).filter((e) => e !== ' ');
  let out = [];
  words.forEach((w) => {
    let o = {};
    o[w] = 1;
    out.push(o);
  });
  return out;
};

const reduceFun = (key, values) => {
  let out = {};
  out[key] = values.length;
  return out;
};

// let dataset = [
//   {'b1-l1': 'It was the best of times, it was the worst of times,'},
//   {'b1-l2': 'it was the age of wisdom, it was the age of foolishness,'},
//   {'b1-l3': 'it was the epoch of belief, it was the epoch of incredulity,'},
//   {'b1-l4': 'it was the season of Light, it was the season of Darkness,'},
//   {'b1-l5': 'it was the spring of hope, it was the winter of despair,'},
// ];

function generateDataset() {
  // Extended arrays of words for substitution
  const timesSynonyms = ['moments', 'periods', 'epochs', 'eras', 'ages', 'intervals', 'seasons', 'decades'];
  const adjectives = [
    'best', 'worst', 'wise', 'foolish', 'bright', 'dark', 'hopeful', 'desperate', 'beloved', 'dreaded',
    'golden', 'bleak', 'glorious', 'dismal', 'peaceful', 'turbulent', 'prosperous', 'dire', 'thriving', 'tragic',
  ];
  const contexts = [
    'times', 'wisdom', 'belief', 'light', 'hope', 'adventures', 'dreams', 'nightmares', 'visions', 'realities',
  ];
  const states = [
    'joy', 'sorrow', 'upheaval', 'tranquility', 'chaos', 'order', 'creation', 'destruction', 'growth', 'decline',
  ];

  // Generate sentences
  let dataset = [];
  for (let i = 0; i < 500; i++) {
    const adj1 = adjectives[Math.floor(Math.random() * adjectives.length)];
    const adj2 = adjectives[Math.floor(Math.random() * adjectives.length)];
    const time1 = timesSynonyms[Math.floor(Math.random() * timesSynonyms.length)];
    const time2 = timesSynonyms[Math.floor(Math.random() * timesSynonyms.length)];
    const ctx1 = contexts[Math.floor(Math.random() * contexts.length)];
    const ctx2 = contexts[Math.floor(Math.random() * contexts.length)];
    const state1 = states[Math.floor(Math.random() * states.length)];
    const state2 = states[Math.floor(Math.random() * states.length)];

    // eslint-disable-next-line max-len
    const line = `It was the ${adj1} ${state1} of ${time1}, it was the ${adj2} ${state2} of ${time2}, it was the era of ${ctx1}, it was the epoch of ${ctx2}.`;
    dataset.push({[`b1-l${i+1}`]: line});
  }
  return dataset;
}

let dataset = generateDataset();
// console.log(dataset.slice(0, 10)); // Display only the first 10 lines to check variability


// const doMapReduce = () => {
//   distribution.test.store.get(null, (e, v) => {
//     distribution.test.mr.exec({keys: v, map: mapFun,
//       reduce: reduceFun}, (e, v) => {
//       terminate();
//     });
//   });
// };

// const doMapReduce = () => {
//     distribution.test.store.get(null, (e, v) => {
//       async.eachLimit(v, 10, (key, done) => {
//         distribution.test.mr.exec({keys: v, map: mapFun, reduce: reduceFun}, (execErr, result) => {
//           done(); // Move to the next item without handling errors
//         });
//       }, (err) => {
//         terminate(); // Terminate after all operations, regardless of success or failure
//       });
//     });
//   };

const doMapReduce = () => {
  distribution.test.store.get(null, (err, keys) => {
    let startTime = Date.now();
    async.eachLimit(keys, CURRENTLIMIT, (key, done) => {
      distribution.test.mr.exec({keys: [key], map: mapFun, reduce: reduceFun}, (execErr, result) => {
        if (execErr) {
          console.error(`Error during MapReduce execution for key ${key}:`, execErr);
        }
        done();
      });
    }, (err) => {
      if (err) {
        console.error('Errors occurred during MapReduce operations:', err);
      }
      let endTime = Date.now();
      let duration = (endTime - startTime) / 1000;
      console.log(`MR Duration: ${duration.toFixed(3)} secs`);

      terminate();
    });
  });
};

distribution.node.start((server) => {
  localServer = server;
  const mrConfig = {gid: 'test', hash: id.rendezvousHash};
  startNodes(() => {
    groupsTemplate(mrConfig).put(mrConfig, mrGroup, (e, v) => {
      async.eachLimit(dataset, CURRENTLIMIT, (data, done) => {
        let key = Object.keys(data)[0];
        let value = data[key];
        distribution.test.store.put(value, key, (err) => {
          done(); // Simply move to the next item
        });
      }, (err) => {
        doMapReduce();
      });
    });
  });
});


// distribution.node.start((server) => {
//   localServer = server;
//   const mrConfig = {gid: 'test', hash: id.rendezvousHash};
//   startNodes(() => {
//     groupsTemplate(mrConfig).put(mrConfig, mrGroup, (e, v) => {
//       let cntr = 0;
//       // We send the dataset to the cluster
//       dataset.forEach((o) => {
//         let key = Object.keys(o)[0];
//         let value = o[key];
//         distribution.test.store.put(value, key, (e, v) => {
//           if (!e) {
//             cntr++;
//             // Once we are done, run the map reduce
//             if (cntr === dataset.length) {
//               doMapReduce();
//             }
//           }
//         });
//       });
//     });
//   });
// });
