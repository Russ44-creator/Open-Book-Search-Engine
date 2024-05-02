//  ________________________________________
// / NOTE: You should use absolute paths to \
// | make sure they are agnostic to where   |
// | your code is running from! Use the     |
// \ `path` module for that purpose.        /
//  ----------------------------------------
//         \   ^__^
//          \  (oo)\_______
//             (__)\       )\/\
//                 ||----w |
//                 ||     ||

const node = global.nodeConfig;
const id = require('../util/id');
const fs = require('fs');
const path = require('path');
const serialization = require('../util/serialization');

const store = {};

const basePath = path.join(__dirname, '../../store');
if (!fs.existsSync(basePath)) {
  fs.mkdirSync(basePath);
}

const dirPath = path.join(__dirname, '../../store', id.getSID(node));
if (!fs.existsSync(dirPath)) {
  fs.mkdirSync(dirPath);
}

function getConfig(config, object) {
  let configuration = {};
  if (typeof config === 'string') {
    configuration.key = config;
    configuration.gid = 'local';
  } else if (typeof config === 'object') {
    configuration = config || {};
  }
  if (object) {
    configuration.key = configuration.key || id.getID(object);
  }
  configuration.gid = configuration.gid || 'local';
  return configuration;
}

function createGroupFolder(groupName) {
  if (!groupName) return;
  const dirPath = path.join(__dirname, '../../store', id.getSID(node),
      groupName);
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath);
  }
}

store.put = function(object, config, callback) {
  callback = callback || function() {};

  config = getConfig(config, object);
  createGroupFolder(config.gid);
  const filePath = path.join(__dirname, '../../store', id.getSID(node),
      config.gid, config.key);
  fs.writeFileSync(filePath, serialization.serialize(object));
  callback(null, object);
};

store.append = function(object, configuration, callback) {
  callback = callback || function() {};

  if (!Array.isArray(object)) {
    object = [object];
  }

  configuration = getConfig(configuration, object);

  const filename = path.join(__dirname, '../../store', id.getSID(node),
      configuration.gid, configuration.key);

  try {
    fs.accessSync(filename);

    // File exists, read and update its contents
    let data = fs.readFileSync(filename, 'utf8');
    data = serialization.deserialize(data);

    let serialized;
    object.forEach((o) => {
      if (Array.isArray(data)) {
        data.push(o);
        serialized = serialization.serialize(data);
      } else {
        let list = [data, o];
        serialized = serialization.serialize(list);
      }
    });

    fs.writeFileSync(filename, serialized);
  } catch (err) {
    // File doesn't exist or there's an error accessing it
    let serialized = serialization.serialize(object);
    fs.writeFileSync(filename, serialized);
  }

  // If you need to return something, you can do it here
  callback(null, object);
};

store.appendAll = function(objectDict, configuration, callback) {
  callback = callback || function() { };
  configuration = getConfig(configuration);

  let count = 0;
  let allObjects = [];
  let keys = Object.keys(objectDict);
  if (count === Object.keys(objectDict).length) {
    callback(null, allObjects);
  }
  for (let key of keys) {
    store.append(objectDict[key],
        {
          key: key, gid: configuration.gid,
        },
        (e, v) => {
          if (v) {
            count++;
            allObjects.push(v);
            if (count === Object.keys(objectDict).length) {
              callback(null, allObjects);
            }
          }
        });
  }
};

store.get = function(config, callback) {
  callback = callback || function() {};
  config = getConfig(config);
  createGroupFolder(config.gid);

  if (config.key == null) {
    const filePath = path.join(__dirname, '../../store', id.getSID(node),
        config.gid);
    fs.readdir(filePath, (err, files) => {
      if (err) {
        console.log(err);
      }
      // console.log('LOCAL STORE GET: ', files);
      callback(null, files);
    });
  } else {
    const filePath = path.join(__dirname, '../../store', id.getSID(node),
        config.gid, config.key);
    if (fs.existsSync(filePath)) {
      const content = fs.readFileSync(filePath, 'utf8');
      callback(null, serialization.deserialize(content));
    } else {
      callback(new Error('Invalid Name'), null);
    }
  }
};

store.del = function(config, callback) {
  callback = callback || function() {};
  config = getConfig(config);
  createGroupFolder(config.gid);

  const filePath = path.join(__dirname, '../../store', id.getSID(node),
      config.gid, config.key);
  if (fs.existsSync(filePath)) {
    const content = fs.readFileSync(filePath, 'utf8');
    fs.unlinkSync(filePath);
    callback(null, serialization.deserialize(content));
  } else {
    callback(new Error('Invalid Name'), null);
  }
};

module.exports = store;
