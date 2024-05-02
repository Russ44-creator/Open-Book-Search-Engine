const natural = require('natural');
const distribution = require('../distribution');
const id = distribution.util.id;

process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0;
global.nodeConfig = {ip: '127.0.0.1', port: 7070};

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

let indexMap = (fileName, obj) => {
  if (!obj || !Array.isArray(obj) || obj.length === 0) {
    return [];
  }

  const {title, author, language, url} = obj[0];
  if (!title || !author || !language || !url) {
    return [];
  }

  const createNGrams = (words, n) => {
    const ngrams = [];
    for (let i = 0; i < words.length - n + 1; i++) {
      ngrams.push(words.slice(i, i + n).join(' '));
    }
    return ngrams;
  };

  const calculateTermFrequencies = (content) => {
    const tokenizer = new natural.WordTokenizer();
    const words = tokenizer.tokenize(content.toLowerCase());
    return words.concat(createNGrams(words, 2)).reduce((acc, term) => {
      acc[term] = (acc[term] || 0) + 1;
      return acc;
    }, {});
  };

  const titleData = calculateTermFrequencies(title);
  const authorData = calculateTermFrequencies(author);

  const output = [];
  [titleData, authorData].forEach((data, index) => {
    const tfLabel = index === 0 ? 'titleTF' : 'authorTF';
    Object.entries(data).forEach(([term, count]) => {
      let entry = output.find((o) => o.hasOwnProperty(term));
      if (!entry) {
        entry = {[term]: {url, title, author, language}};
        output.push(entry);
      }
      entry[term][tfLabel] = count / Object.keys(data).length;
    });
  });

  return output;
};

let indexReduce = (term, values) => {
  if (!values) {
    return {};
  }

  const N = 7000;
  const calculateIDF = (documentCount) =>
    documentCount > 0 ? 1 + Math.log(N / documentCount) : 0;

  const processEntries = (entries, tfLabel) => entries.filter((v) => v && v[tfLabel] !== undefined)
      .map((entry) => ({...entry, score: entry[tfLabel] * calculateIDF(entries.length)}));

  const titleScores = processEntries(values, 'titleTF');
  const authorScores = processEntries(values, 'authorTF');

  return {term, titleScores, authorScores};
};

const doIndexMapReduce = (cb) => {
  distribution.crawler.store.get(null, (e, v) => {
    distribution.crawler.mr.exec(
        {keys: v, map: indexMap, reduce: indexReduce, storeReducedValue: true},
        (e, v) => {
          if (e) {
            console.error('Map-reduce execution error:', e);
            return;
          }
          terminate();
        },
    );
  });
};

distribution.node.start((server) => {
  localServer = server;
  const crawlerConfig = {gid: 'crawler'};
  startNodes(() => {
    groupsTemplate(crawlerConfig).put(crawlerConfig, crawlerGroup, (e, v) => {
      doIndexMapReduce();
    });
  });
});
