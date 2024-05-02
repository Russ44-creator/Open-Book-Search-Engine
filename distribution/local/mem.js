const id = require('../util/id');

const inMemStore = new Map();
const mem = {};

mem.get = function(keyObj, callback) {
  let key;
  let gid;
  if (keyObj === null || typeof keyObj === 'string') {
    key = keyObj;
    gid = 'local';
  } else {
    key = keyObj.key;
    gid = keyObj.gid;
  }

  if (key === null) {
    let allKeys = [];
    const store = inMemStore.get(gid);
    if (!store) {
      callback(null, allKeys);
      return;
    }
    for (let key of store.keys()) {
      allKeys.push(key);
    }
    callback(null, allKeys);
    return;
  } else {
    const store = inMemStore.get(gid);

    if (!store) {
      callback(new Error('Group ID not found'), null);
      return;
    }

    if (store.has(key)) {
      callback(null, store.get(key));
    } else {
      callback(new Error('Key not found'), null);
    }
  }
};


mem.put = function(object, keyObj, callback) {
  let key;
  let gid;
  if (keyObj === null || typeof keyObj === 'string') {
    key = keyObj;
    gid = 'local';
  } else {
    key = keyObj.key;
    gid = keyObj.gid;
  }

  let gidStore = inMemStore.get(gid);

  if (!gidStore) {
    gidStore = new Map();
    inMemStore.set(gid, gidStore);
  }

  const finalKey = key || id.getID(object);
  gidStore.set(finalKey, object);
  callback(null, object);
};

mem.del = function(keyObj, callback) {
  let key;
  let gid;
  if (keyObj === null || typeof keyObj === 'string') {
    key = keyObj;
    gid = 'local';
  } else {
    key = keyObj.key;
    gid = keyObj.gid;
  }

  const gidStore = inMemStore.get(gid);

  if (gidStore && gidStore.has(key)) {
    const obj = gidStore.get(key);
    gidStore.delete(key);
    if (gidStore.size === 0) {
      inMemStore.delete(gid);
    }
    callback(null, obj);
  } else {
    callback(new Error('Key not found'), null);
  }
};

module.exports = mem;
