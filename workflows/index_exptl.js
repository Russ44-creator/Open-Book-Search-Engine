const distribution = require('../distribution');
const id = distribution.util.id;
const groupsTemplate = require('../distribution/all/groups');
const booksGroup = {};

let localServer = null;
global.nodeConfig = {ip: '127.0.0.1', port: 7070};

const n1 = {ip: '127.0.0.1', port: 7110};
const n2 = {ip: '127.0.0.1', port: 7111};
const n3 = {ip: '127.0.0.1', port: 7112};

booksGroup[id.getSID(n1)] = n1;
booksGroup[id.getSID(n2)] = n2;
booksGroup[id.getSID(n3)] = n3;

const startNodes = (cb) => {
  distribution.local.status.spawn(n1, (e, v) => {
    distribution.local.status.spawn(n2, (e, v) => {
      distribution.local.status.spawn(n3, (e, v) => {
        cb();
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
      localServer.close();
    });
  });
};

let m1 = (url, content) => {
  const termFrequency = {};
  const words = content.toLowerCase().match(/\w+/g) || [];
  const totalWords = words.length;
  const output = [];

  words.forEach((word) => {
    termFrequency[word] = (termFrequency[word] || 0) + 1;
  });

  for (const [term, count] of Object.entries(termFrequency)) {
    // Normalize term frequency by the total number of words in the document
    const normalizedFrequency = count / totalWords;
    let o = {};
    o[term] = {url: url, tf: normalizedFrequency};
    output.push(o);
  }

  return output;
};

let r1 = (term, values) => {
  const N = 3;
  let out = {};
  let idf = 1 + Math.log(N / values.length);
  let scores = values.map((entry) => ({url: entry.url, score: entry.tf * idf}));

  console.log('term: ', term);
  console.log('tf: ', values);
  console.log('idf: ', idf);
  console.log('scores: ', scores);
  out = {
    tf: values,
    idf: idf,
    score: scores,
  };
  return out;
};

let dataset = [
  {document1: 'Machine learning teaches machine how to learn'},
  {document2: 'Machine translation is my favorite subject'},
  {document3: 'Term frequency and inverse document frequency is important'},
];

const doMapReduce = (cb) => {
  distribution.books.store.get(null, (e, v) => {
    distribution.books.mr.exec({keys: v, map: m1, reduce: r1, storeReducedValue: true}, (e, v) => {
      terminate();
    });
  });
};

let cntr = 0;


distribution.node.start((server) => {
  localServer = server;
  const booksConfig = {gid: 'books'};
  startNodes(() => {
    groupsTemplate(booksConfig).put(booksConfig, booksGroup, (e, v) => {
      dataset.forEach((o) => {
        let key = Object.keys(o)[0];
        let value = o[key];
        distribution.books.store.put(value, key, (e, v) => {
          cntr++;
          if (cntr === dataset.length) {
            doMapReduce();
          }
        });
      });
    });
  });
});
