import os
import psycopg2
import time
import requests
import wget
from fastapi import UploadFile

DB_CONNECTION = os.environ["DB_URL"]
WHISPER_API_URL = os.environ["WHISPER_API_URL"]

def connect_to_db():
    print("Connecting to the database...")
    return psycopg2.connect(DB_CONNECTION)

def get_latest_id(conn):
    print("Getting the latest record ID...")
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(id) FROM terra_audio;")
        return cur.fetchone()[0]

def get_newest_records(conn, last_processed_id):
    print("Getting new records...")
    with conn.cursor() as cur:
        cur.execute("SELECT id, url, title FROM terra_audio WHERE id > %s;", (last_processed_id,))
        return cur.fetchall()

def call_api_with_audio_file(audio_file_path):
    print("Transcribing...")
    url = WHISPER_API_URL
    params = {"task": "transcribe", "language": "en", "output": "txt"}

    with open(audio_file_path, "rb") as audio_file:
        return requests.post(url, files={"audio_file": (audio_file_path, audio_file, "audio/mpeg")}, params=params)


def process_records(records, conn):
    print("Processing records...")
    for record_id, url, title in records:

        print(f"Downloading title: {title}")
        audio_file_path = wget.download(url)
        conn.close()
        response = call_api_with_audio_file(audio_file_path)
        print(f"Transcription complete")

        conn = connect_to_db()
        transcript = response.text

        with conn.cursor() as cur:
            cur.execute("UPDATE terra_audio SET transcript = %s WHERE id = %s;", (transcript, record_id))
        conn.commit()
        os.remove(audio_file_path)
    conn.close()


def job(initial_id, last_processed_id):
    conn = connect_to_db()
    latest_id = get_latest_id(conn)
    print(f"Latest ID: {latest_id}")

    if latest_id > max(initial_id, last_processed_id):
        records = get_newest_records(conn, max(initial_id, last_processed_id))
        print(f"Number of new records: {len(records)}")
        process_records(records, conn)  # Pass conn as an argument
        last_processed_id = latest_id
    else:
        print("Nothing new found.")
    
    conn.close()
    return last_processed_id


if __name__ == "__main__":
    print("Starting the scheduler!")
    last_processed_id = 0
    initial_id = get_latest_id(connect_to_db())
    SLEEP_INTERVAL = 120  # 2 minutes

    while True:
        last_processed_id = job(initial_id, last_processed_id)
        print(f"Sleeping for {SLEEP_INTERVAL} seconds...")
        time.sleep(SLEEP_INTERVAL)
