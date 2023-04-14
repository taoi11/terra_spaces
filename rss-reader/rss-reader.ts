import axios from 'axios';
import xml2js from 'xml2js';
import { Client } from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const DB_CONNECTION = process.env.DB_URL as string;

async function getRssFeed() {
  console.log('Fetching RSS feed...');
  const url = 'https://terraspaces.org/feed/podcast/';
  const response = await axios.get(url);
  const data = await xml2js.parseStringPromise(response.data as string);
  return data.rss.channel[0].item;
}

function connectToDb() {
  console.log('Connecting to the database...');
  return new Client({
    connectionString: DB_CONNECTION,
  });
}

async function fetchExistingUrls(client: Client) {
  console.log('Fetching existing URLs...');
  const res = await client.query('SELECT url FROM terra_spaces ORDER BY id DESC LIMIT 250;');
  return res.rows.map((row) => row.url);
}

async function insertNewEntry(client: Client, title: string, url: string) {
  console.log(`Inserting new entry: ${title} (${url})`);
  await client.query('INSERT INTO terra_spaces (title, url) VALUES ($1, $2);', [title, url]);
}

async function rssReader() {
  const client = connectToDb();
  await client.connect();

  const existingUrls = await fetchExistingUrls(client);
  const rssItems = await getRssFeed();

  console.log(`Fetched ${rssItems.length} items from RSS feed.`);
  console.log(`Found ${existingUrls.length} existing URLs in the database.`);

  const newEntries: string[] = [];

  for (const item of rssItems) {
    const title = item.title[0];
    const enclosureUrl = item.enclosure[0]['$'].url;

    if (!existingUrls.includes(enclosureUrl)) {
      await insertNewEntry(client, title, enclosureUrl);
      newEntries.push(`Inserted new entry: ${title} (${enclosureUrl})`);
    }
  }

  await client.end();
  return newEntries;
}

async function main() {
  console.log('Starting main function...');
  const newEntries = await rssReader();
  newEntries.reverse();
  for (const entry of newEntries) {
    console.log(entry);
  }
  console.log('Main function completed. Scheduling next run...');
  setTimeout(main, 60 * 60);
}

main();