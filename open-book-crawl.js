const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');
global.fetch = require('node-fetch');
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0;
const BASE_URL = 'http://www.gutenberg.org'; // This should be the base URL of a Gutenberg mirror if you're using one.

async function fetchHTML(url) {
  try {
    const response = await global.fetch(url);
    if (!response.ok) throw new Error(`Failed to fetch URL: ${url}`);
    return await response.text();
  } catch (error) {
    console.error(`Error fetching page: ${error.message}`);
    return null;
  }
}

async function getBookLinks(url) {
  const html = await fetchHTML(url);
  console.log(html);
  if (!html) return [];

  const $ = cheerio.load(html);
  const links = [];
  $('a').each((_, element) => {
    const href = $(element).attr('href');
    if (href && href.includes('/ebooks/')) {
      links.push(new URL(href, BASE_URL).href);
    }
  });
  console.log(links);
  return links;
}

async function downloadBook(url) {
  const html = await fetchHTML(url);
  if (!html) return;

  const $ = cheerio.load(html);
  $('a').each(async (_, element) => {
    const href = $(element).attr('href');
    if (href && href.endsWith('.txt')) {
      const textUrl = new URL(href, url).href;
      const response = await fetch(textUrl);
      const textData = await response.text();

      const filename = path.basename(href);
      fs.writeFile(`./books/${filename}`, textData, 'utf8', (err) => {
        if (err) console.error(`Error writing file: ${err}`);
        else console.log(`Book saved: ${filename}`);
      });
    }
  });
}

async function main() {
  // const url = `${BASE_URL}/browse/scores/top`;
  const url = 'https://atlas.cs.brown.edu/data/gutenberg/';
  try {
    const response = await global.fetch(url);
    const html = await response.text();
    console.log('HTML: ', html);
  } catch (error) {
    console.error('Error extracting text from URL:', error);
  }

  const bookLinks = await getBookLinks(url);

  // Ensure the books directory exists
  if (!fs.existsSync('./books')) {
    fs.mkdirSync('./books');
  }

  console.log('Length ', bookLinks.length);

  // for (let bookLink of bookLinks) {
  //   await downloadBook(bookLink);
  // }
}

main().catch(console.error);
