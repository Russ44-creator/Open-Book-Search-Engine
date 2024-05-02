global.fetch = require('node-fetch');
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0;

global.nodeConfig = {ip: '127.0.0.1', port: 7070};
const distribution = require('../distribution');
const id = distribution.util.id;

const groupsTemplate = require('../distribution/all/groups');

const crawlerGroup = {};

/*
   This hack is necessary since we can not
   gracefully stop the local listening node.
   The process that node is
   running in is the actual jest process
*/
let localServer = null;

/*
    The local node will be the orchestrator.
*/

const n1 = {ip: '127.0.0.1', port: 7110};
const n2 = {ip: '127.0.0.1', port: 7111};
const n3 = {ip: '127.0.0.1', port: 7112};
const n4 = {ip: '127.0.0.1', port: 7113};
const n5 = {ip: '127.0.0.1', port: 7114};


crawlerGroup[id.getSID(n1)] = n1;
crawlerGroup[id.getSID(n2)] = n2;
crawlerGroup[id.getSID(n3)] = n3;
crawlerGroup[id.getSID(n4)] = n4;
crawlerGroup[id.getSID(n5)] = n5;


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

const terminate = () => {
  console.log('-------------NODES CLEANING----------');
  let remote = {service: 'status', method: 'stop'};
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        remote.node = n4;
        distribution.local.comm.send([], remote, (e, v) => {
          remote.node = n5;
          distribution.local.comm.send([], remote, (e, v) => {
            localServer.close();
          });
        });
      });
    });
  });
};

const SEARCH_TERM = 'porter';

const findClosestScores = (targetScore, candidates) => {
  candidates.sort((a, b) => {
    const diffA = Math.abs(targetScore - a.score);
    const diffB = Math.abs(targetScore - b.score);
    return diffA - diffB;
  });

  return candidates.slice(0, 3).map(candidate => candidate.url);
};


distribution.node.start((server) => {
  localServer = server;
  const crawlerConfig = {gid: 'crawler'};
  startNodes(() => {
    groupsTemplate(crawlerConfig).put(crawlerConfig, crawlerGroup, (e, v) => {
      distribution.crawler.store.get(`${SEARCH_TERM}`, (e, v) => {
        console.log('@@@@');
        console.log(e);
        console.log(v);
        if (e) {
          console.error('Error retrieving data:', e);
          terminate();
          return;
        }

        // Since we can only search 1 term now, the tfidf score for query in cosSim is always 1
        if (v.titleScores) {
          console.log('The result document with a matching title is: ');
          console.log(findClosestScores(1, v.titleScores));
        }

        if (v.authorScores) {
          console.log('The result document with a matching author is: ');
          console.log(findClosestScores(1, v.authorScores));
        }

        terminate();
      });
    });
  });
});
