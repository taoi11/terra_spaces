import requests
import xml.etree.ElementTree as ET
import psycopg2
import os
import time

DB_CONNECTION = os.environ['DB_URL']

def get_rss_feed():
    print('Fetching RSS feed...')
    url = 'https://terraspaces.org/feed/podcast/'
    response = requests.get(url)
    data = ET.fromstring(response.content)
    return data.findall('./channel/item')

def connect_to_db():
    print('Connecting to the database...')
    return psycopg2.connect(DB_CONNECTION)

def fetch_existing_urls(conn):
    print('Fetching existing URLs...')
    with conn.cursor() as cur:
        cur.execute('SELECT url FROM terra_audio ORDER BY id DESC LIMIT 250;')
        return [row[0] for row in cur.fetchall()]

def insert_new_entry(conn, title, url):
    print(f'Inserting new entry: {title} ({url})')
    with conn.cursor() as cur:
        cur.execute('INSERT INTO terra_audio (title, url) VALUES (%s, %s);', (title, url))
    conn.commit()

def rss_reader():
    conn = connect_to_db()

    existing_urls = fetch_existing_urls(conn)
    rss_items = get_rss_feed()

    print(f'Fetched {len(rss_items)} items from RSS feed.')
    print(f'Found {len(existing_urls)} existing URLs in the database.')

    new_entries = []

    # Reverse the order of RSS items to insert the newest one last
    rss_items.reverse()

    for item in rss_items:
        title = item.find('title').text
        enclosure_url = item.find('enclosure').get('url')

        if enclosure_url not in existing_urls:
            insert_new_entry(conn, title, enclosure_url)
            new_entries.append(f'Inserted new entry: {title} ({enclosure_url})')

    conn.close()
    return new_entries

def main():
    print('Starting main function...')
    new_entries = rss_reader()
    new_entries.reverse()
    for entry in new_entries:
        print(entry)
    print('Main function completed. Scheduling next run...')
    time.sleep(60 * 60)
    main()

if __name__ == '__main__':
    main()
    