
const idfValues = {
  'machine': 0.7,
  'learning': 1.05,
};

function calculateTfIdf(query) {
  const words = query.toLowerCase().split(' ');
  const tfValues = {};
  const tfIdfResults = {};

  // Calculate TF
  words.forEach((word) => {
    tfValues[word] = (tfValues[word] || 0) + 1;
  });

  const wordCount = words.length;
  Object.keys(tfValues).forEach((word) => {
    tfValues[word] = tfValues[word] / wordCount;
  });

  // Calculate TF-IDF and format results
  const results = [];
  Object.keys(tfValues).forEach((word) => {
    const tf = tfValues[word];
    const idf = idfValues[word] || 0; // default IDF to 0 if word is not found
    const tfIdf = tf * idf;
    results.push(`${word}: TF: ${tf.toFixed(2)}, IDF: ${idf}, TF*IDF: ${tfIdf.toFixed(2)}`);
    tfIdfResults[word] = tfIdf;
  });

  console.log('Results: ', results);
  return tfIdfResults;
}

const query = 'machine learning';
const tfIdfScores = calculateTfIdf(query);
console.log('TF-IDF Vector Scores: ', tfIdfScores);


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

const SEARCH_TERM = 'machine';
const cosSim = (score, candidates) => {
  let closest = candidates[0];
  let minDiff = Math.abs(score - closest.score);

  for (let i = 1; i < candidates.length; i++) {
    let currentDiff = Math.abs(score - candidates[i].score);
    if (currentDiff < minDiff) {
      closest = candidates[i];
      minDiff = currentDiff;
    }
  }

  return closest.url;
};

distribution.node.start((server) => {
  localServer = server;
  const booksConfig = {gid: 'books'};
  startNodes(() => {
    groupsTemplate(booksConfig).put(booksConfig, booksGroup, (e, v) => {
      distribution.books.store.get(`${SEARCH_TERM}`, (e, v) => {
        console.log('The result document is: ');
        console.log(cosSim(tfIdfScores[SEARCH_TERM], v.score));
        terminate();
      });
    });
  });
});
