global.fetch = require('node-fetch');
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0;

global.nodeConfig = {ip: '0.0.0.0', port: 0};
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

// const n1 = {ip: '13.58.185.159', port: 7110};
const n2 = {ip: '3.129.249.87', port: 7110};
// const n3 = {ip: '127.0.0.1', port: 7112};
// const n4 = {ip: '127.0.0.1', port: 7113};
// const n5 = {ip: '127.0.0.1', port: 7114};


// crawlerGroup[id.getSID(n1)] = n1;
crawlerGroup[id.getSID(n2)] = n2;
// crawlerGroup[id.getSID(n3)] = n3;
// crawlerGroup[id.getSID(n4)] = n4;
// crawlerGroup[id.getSID(n5)] = n5;


const startNodes = (cb) => {
  // distribution.local.status.spawn(n1, (e, v) => {
    distribution.local.status.spawn(n2, (e, v) => {
      // distribution.local.status.spawn(n3, (e, v) => {
      //   distribution.local.status.spawn(n4, (e, v) => {
      //     distribution.local.status.spawn(n5, (e, v) => {
            cb();
          });
        // });
    //   });
    // });
  // });
};

let dataset = [
  // {'000': 'https://atlas.cs.brown.edu/data/gutenberg/1/'},
  // {'001': 'https://atlas.cs.brown.edu/data/gutenberg/2'},
  {'000': 'https://atlas.cs.brown.edu/data/gutenberg/1/1/1/'},
  // {'002': 'https://atlas.cs.brown.edu/data/gutenberg/1/2/'},
  // {'003': 'https://atlas.cs.brown.edu/data/gutenberg/1/3/'},
  // {'004': 'https://atlas.cs.brown.edu/data/gutenberg/1/4/'},
  // {'005': 'https://atlas.cs.brown.edu/data/gutenberg/1/5/'},

];

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


let mapCrawlParent = async (key, value) => {
  const response = await global.fetch(value);
  const content = await response.text();
  // console.log('content: ', content);

  const dom = new global.JSDOM(content);
  const baseURL = value;
  const anchorElements = Array.from(dom.window.document.querySelectorAll('a'));
  let out = [];
  anchorElements.map((a) => {
    const href = a.getAttribute('href');
    let o = {};
    let hrefKey = href.toString().replace(/[^a-zA-Z0-9_-]/g, '');
    // check it has data or CDOA, CMOA, CNOD, CSOA,
    const isDataOrOneOf = hrefKey.includes('data') || ['CDOA', 'CMOA', 'CNOD', 'CSOA'].includes(hrefKey);
    if (isDataOrOneOf == false) {
      o[hrefKey] = new URL(href, baseURL).toString();
      out.push(o);
    }
  });

  return out;
};

let reduceCrawlParent = (key, values) => {
  // check txt
  if (values[0].includes('txt') == false) {
    return key;
  } else {
    return null;
  }
};

let mapCrawlChild = async (key, values) => {
  let out = [];
  // console.log('Extract URLs Key and Values: ', key, values);
  async function fetchWithTimeout(resource, options = {}) {
    const {timeout = 8000} = options;

    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeout);

    const response = await fetch(resource, {
      ...options,
      signal: controller.signal,
    });
    clearTimeout(id);

    return response;
  }
  for (value of values) {
    const baseURL = value;
    try {
      const response = await fetchWithTimeout(value, {
        timeout: 600000,
      });
      const content = await response.text();
      const dom = new global.JSDOM(content);
      const anchorElements = Array.from(dom.window.document.querySelectorAll('a'));
      // console.log('anchorElements: ', anchorElements);

      anchorElements.map((a) => {
        const href = a.getAttribute('href');
        let o = {};
        let hrefKey = href.toString().replace(/[^a-zA-Z0-9_-]/g, '');
        // check it has data or CDOA, CMOA, CNOD, CSOA,
        const isDataOrOneOf = hrefKey.includes('data') || ['CDOA', 'CMOA', 'CNOD', 'CSOA'].includes(hrefKey);
        if (isDataOrOneOf == false) {
          const newURL = new URL(href, baseURL).toString();
          // console.log('NewUrl: ', newURL);
          o[hrefKey] = newURL;
          out.push(o);
        }
      });
    } catch (e) {
      console.log('Extract URLs Fetch Error: ', e);
      continue;
    };
  }
  return out;
};

let mapCrawlText = async (key, values) => {
  let out = {};
  let o = {};
  // console.log('Crawl Text Key and Values: ', key, values);
  baseURL = values[0];
  async function fetchWithTimeout(resource, options = {}) {
    const {timeout = 8000} = options;

    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeout);

    const response = await fetch(resource, {
      ...options,
      signal: controller.signal,
    });
    clearTimeout(id);

    return response;
  }
  try {
    const response = await fetchWithTimeout(values[0], {
      timeout: 600000,
    });
    // const response = await global.fetch(baseURL);
    const content = await response.text();
    // find the language
    let languageIndex = content.indexOf('Language: ');
    if (languageIndex !== -1) {
      let newlineIndex = content.indexOf('\n', languageIndex);
      if (newlineIndex !== -1) {
        let languageField = content.substring(languageIndex + 'Language: '.length, newlineIndex).trim();
        o['language'] = languageField;
      // console.log('Language: ', languageField);
      }
    } else {
      o['language'] = 'Unknown';
    }
    // find the title
    let titleIndex = content.indexOf('Title: ');
    if (titleIndex !== -1) {
      let newlineIndex = content.indexOf('\n', titleIndex);
      if (newlineIndex !== -1) {
        let titleField = content.substring(titleIndex + 'Title: '.length, newlineIndex).trim();
        o['title'] = titleField;
      // console.log('Title: ', titleField);
      }
    } else {
      o['title'] = 'Unknown';
    }
    // find the author
    let authorIndex = content.indexOf('Author: ');
    if (authorIndex!== -1) {
      let newlineIndex = content.indexOf('\n', authorIndex);
      if (newlineIndex!== -1) {
        let authorField = content.substring(authorIndex + 'Author: '.length, newlineIndex).trim();
        o['author'] = authorField;
      // console.log('Author: ', authorField);
      }
    } else {
      o['author'] = 'Anonymous';
    }

    // let sanitizeContent = content.toString().replace(/[^a-zA-Z0-9\s?!,;.]/g, ' ');
    // clean the \r and \n
    // sanitizeContent = sanitizeContent.replace(/\r?\n|\r/g, '');
    o['url'] = values[0];
    // o['content'] = sanitizeContent;
    out[key] = o;
  } catch (error) {
    console.error('Crawl Text Fetch error:', error);
    // fetch error and we could store the urls and crawl again
    return out;
  }
  return out;
};

let reduceCrawlText = (key, values) => {
  return values;
};

const doMapReduce = () => {
  distribution.crawler.store.get(null, (e, v) => {
    console.log('Values and Error: ', e, v);
    distribution.crawler.mr.exec({keys: v, map: mapCrawlParent,
      reduce: reduceCrawlParent, storeReducedValue: false}, (e, v) => {
      if (v.length != 0) {
        console.log('Crawl Again!!!!!!');
        setTimeout(function() {
          doCrawlURL(v);
        }, 20);
      } else {
        console.log('Crawl Text!!!!!!');
        doCrawlText();
      }
    });
  });
};

const doCrawlURL = (urlKey) => {
  distribution.crawler.mr.exec({keys: urlKey, map: mapCrawlChild,
    reduce: reduceCrawlParent, storeReducedValue: false}, (e, v) => {
    if (v.length != 0) {
      console.log('Crawl Again!!!!!!');
      setTimeout(function() {
        doCrawlURL(v);
      }, 20);
    } else {
      console.log('Crawl Text!!!!!!');
      doCrawlText();
    }
  });
};

const doCrawlText = () => {
  // Get the all the text urls from the local store
  distribution.crawler.store.get(null, (e, v) => {
    console.log('Crawler Text Values length and Error: ', e, v.length);
    distribution.crawler.mr.exec({keys: v, map: mapCrawlText,
      reduce: reduceCrawlText, storeReducedValue: false}, (e, v) => {
      terminate();
    });
  });
};

distribution.node.start((server) => {
  localServer = server;
  const crawlerConfig = {gid: 'crawler'};
  // startNodes(() => {
    groupsTemplate(crawlerConfig).put(crawlerConfig,
        crawlerGroup, (e, v) => {
          console.log('Put nodes into group: ', e, v);
          let cntr = 0;
          // We send the dataset to the cluster
          dataset.forEach((o) => {
            let key = Object.keys(o)[0];
            let value = o[key];
            distribution.crawler.store.put(value, key, (e, v) => {
              if (!e) {
                cntr++;
                // Once we are done, run the map reduce
                if (cntr === dataset.length) {
                  doMapReduce();
                }
              }
            });
          });
        });
  // });
});


// distribution.node.start((server) => {
//   localServer = server;
//   const crawlerConfig = {gid: 'crawler'};
//   startNodes(() => {
//     groupsTemplate(crawlerConfig).put(crawlerConfig,
//         crawlerGroup, (e, v) => {
//           distribution.crawler.store.get(null, (e, v) => {
//             console.log('Crawler Text Values length and Error: ', e, v.length);
//             // distribution.crawler.mr.exec({keys: v, map: mapCrawlText,
//             //   reduce: reduceCrawlText, storeReducedValue: false}, (e, v) => {
//             terminate();
//           });
//         });
//   });
// });

