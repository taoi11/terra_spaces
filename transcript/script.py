import os
import psycopg2
import time
import requests
import wget

DB_CONNECTION = os.environ["DB_URL"]
TRANSCRIBE_API_URL = os.environ["TRANSCRIBE_API_URL"]
TRANSCRIPTS_FOLDER = "/output"

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
        cur.execute("SELECT url, title FROM terra_spaces WHERE id > %s;", (last_processed_id,))
        return cur.fetchall()

def call_api_with_audio_file(audio_file_path):
    print("Transcribing...")
    url = TRANSCRIBE_API_URL
    params = {"task": "transcribe", "language": "en", "output": "txt"}
    
    with open(audio_file_path, "rb") as audio_file:
        files = {"audio_file": audio_file}
        return requests.post(url, files=files, params=params)

def process_records(records):
    print("Processing records...")
    for url, title in records:
        print(f"Downloading title: {title}")

        audio_file_path = wget.download(url)
        response = call_api_with_audio_file(audio_file_path)
        print(f"Transcription complete")

        text_file_path = f"{TRANSCRIPTS_FOLDER}/{os.path.splitext(title)[0]}.txt"
        with open(text_file_path, "w") as text_file:
            text_file.write(response.text)

        os.remove(audio_file_path)

def job(initial_id, last_processed_id):
    conn = connect_to_db()
    latest_id = get_latest_id(conn)
    print(f"Latest ID: {latest_id}")

    if latest_id > max(initial_id, last_processed_id):
        records = get_newest_records(conn, max(initial_id, last_processed_id))
        print(f"Number of new records: {len(records)}")
        process_records(records)
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
