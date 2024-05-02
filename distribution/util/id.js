const assert = require('assert');
var crypto = require('crypto');

// The ID is the SHA256 hash of the JSON representation of the object
function getID(obj) {
  const hash = crypto.createHash('sha256');
  hash.update(JSON.stringify(obj));
  return hash.digest('hex');
}

// The NID is the SHA256 hash of the JSON representation of the node
function getNID(node) {
  node = {ip: node.ip, port: node.port};
  return getID(node);
}

// The SID is the first 5 characters of the NID
function getSID(node) {
  return getNID(node).substring(0, 5);
}


function idToNum(id) {
  let n = parseInt(id, 16);
  assert(!isNaN(n), 'idToNum: id is not in KID form!');
  return n;
}

function naiveHash(kid, nids) {
  nids.sort();
  let n = parseInt(kid, 16);
  return nids[n % nids.length];
}

function consistentHash(kid, nids) {
  let hashValue = idToNum(kid);
  nids.sort();
  for (let i = 0; i < nids.length; i++) {
    if (idToNum(nids[i]) >= hashValue) {
      return nids[i];
    }
  }
  return nids[0];
}


function rendezvousHash(kid, nids) {
  let max = 0;
  let index = 0;
  for (let i = 0; i < nids.length; i++) {
    let hash = idToNum(getID(kid + nids[i]));
    if (hash > max) {
      index = i;
      max = hash;
    }
  }
  return nids[index];
}

module.exports = {
  getNID: getNID,
  getSID: getSID,
  getID: getID,
  idToNum: idToNum,
  naiveHash: naiveHash,
  consistentHash: consistentHash,
  rendezvousHash: rendezvousHash,
};
