global.fetch = require('node-fetch');

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

beforeAll((done) => {
  crawlerGroup[id.getSID(n1)] = n1;
  crawlerGroup[id.getSID(n2)] = n2;
  crawlerGroup[id.getSID(n3)] = n3;


  const startNodes = (cb) => {
    distribution.local.status.spawn(n1, (e, v) => {
      distribution.local.status.spawn(n2, (e, v) => {
        distribution.local.status.spawn(n3, (e, v) => {
          cb();
        });
      });
    });
  };

  distribution.node.start((server) => {
    localServer = server;

    const crawlerConfig = {gid: 'crawler'};
    startNodes(() => {
      groupsTemplate(crawlerConfig).put(crawlerConfig,
          crawlerGroup, (e, v) => {
            done();
          });
    });
  });
});


afterAll((done) => {
  let remote = {service: 'status', method: 'stop'};
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        localServer.close();
        done();
      });
    });
  });
});


test('(25 pts) crawler test', (done) => {
  let m1 = async function(key, url) {
    let o = {};
    try {
      const response = await global.fetch(url);
      const html = await response.text();
      o[url] = html;
    } catch (error) {
      console.error('Error extracting text from URL:', error);
    }
    return o;
  };

  let r1 = (key, value) => {
    let out = {};
    out[key] = value;
    return out;
  };

  let dataset = [{0: 'http://example.com'}];

  // const contentPath = path.join(__dirname, '../search/example-page.txt');
  // const content = fs.readFileSync(contentPath, 'utf8');
  // console.log(content);
  // let expected = [
  //   {
  //     'http://example.com': [],
  //   },
  // ];

  /* Sanity check: map and reduce locally */
  // sanityCheck(m1, r1, dataset, expected, done);

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.crawler.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.crawler.mr.exec({keys: v, map: m1, reduce: r1}, (e, v) => {
        try {
          // expect(v).toEqual(expect.arrayContaining(expected));
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // We send the dataset to the cluster
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.crawler.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});
