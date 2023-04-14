import os
import psycopg2
import time
import requests
import wget
import json

DB_CONNECTION = os.environ["DB_URL"]
TRANSCRIBE_API_URL = os.environ["TRANSCRIBE_API_URL"]

def connect_to_db():
    print("Connecting to the database...")
    return psycopg2.connect(DB_CONNECTION)

def get_latest_id(conn):
    print("Getting the latest record ID...")
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(id) FROM terra_spaces;")
        return cur.fetchone()[0]

def get_newest_records(conn, last_processed_id):
    print("Getting new records...")
    with conn.cursor() as cur:
        cur.execute("SELECT id, url, title FROM terra_spaces WHERE id > %s;", (last_processed_id,))
        return cur.fetchall()

def call_api_with_audio_file(audio_file_path):
    print("Transcribing...")
    url = TRANSCRIBE_API_URL
    params = {"task": "transcribe", "language": "en", "output": "json"}
    
    with open(audio_file_path, "rb") as audio_file:
        files = {"audio_file": audio_file}
        return requests.post(url, files=files, params=params)

def add_transcript(conn, record_id, transcript):
    print("Adding the transcript to the database...")
    with conn.cursor() as cur:
        cur.execute("UPDATE terra_spaces SET transcript = %s WHERE id = %s;", (transcript, record_id))
        conn.commit()

def process_records(conn, records):
    print("Processing records...")
    for record_id, url, title in records:
        print(f"Downloading title: {title}")

        audio_file_path = wget.download(url)
        response = call_api_with_audio_file(audio_file_path)
        print(f"Transcription complete")

        transcript = json.loads(response.text)
        add_transcript(conn, record_id, json.dumps(transcript))

        os.remove(audio_file_path)

def job(initial_id, last_processed_id):
    conn = connect_to_db()
    latest_id = get_latest_id(conn)
    print(f"Latest ID: {latest_id}")

    if latest_id > max(initial_id, last_processed_id):
        records = get_newest_records(conn, max(initial_id, last_processed_id))
        print(f"Number of new records: {len(records)}")
        process_records(conn, records)
        last_processed_id = latest_id
    else:
        print("Nothing new found.")
    
    conn.close()
    return last_processed_id

if __name__ == "__main__":
    print("Starting the scheduler!")
    last_processed_id = 0
    initial_id = get_latest_id(connect_to_db())

    while True:
        last_processed_id = job(initial_id, last_processed_id)
        print("Sleeping for 300 seconds...")
        time.sleep(60*60)
