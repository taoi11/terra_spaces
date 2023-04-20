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

def get_newest_records(conn):
    print("Getting new records...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, url, title, transcript
            FROM terra_audio
            ORDER BY id DESC
            LIMIT 50;
        """)
        return cur.fetchall()

def call_api_with_audio_file(audio_file_path):
    print("Transcribing...")
    url = WHISPER_API_URL
    params = {"task": "transcribe", "language": "en", "output": "txt"}

    with open(audio_file_path, "rb") as audio_file:
        return requests.post(url, files={"audio_file": (audio_file_path, audio_file, "audio/mpeg")}, params=params)

def process_records(records, conn):
    print("Processing records...")
    for record_id, url, title, transcript in records:
        if transcript is None:
            print(f"Downloading title: {title}")
            audio_file_path = wget.download(url)
            conn.close()
            response = call_api_with_audio_file(audio_file_path)
            print(f"Transcription complete")

            conn = connect_to_db()
            new_transcript = response.text

            with conn.cursor() as cur:
                cur.execute("UPDATE terra_audio SET transcript = %s WHERE id = %s;", (new_transcript, record_id))
            conn.commit()
            os.remove(audio_file_path)
    conn.close()

def job():
    conn = connect_to_db()
    records = get_newest_records(conn)
    print(f"Number of new records: {len(records)}")

    if len(records) > 0:
        process_records(records, conn)  # Pass conn as an argument
    else:
        print("Nothing new found.")
    
    conn.close()

if __name__ == "__main__":
    print("Starting the scheduler!")
    SLEEP_INTERVAL = 120  # 2 minutes

    while True:
        job()
        print(f"Sleeping for {SLEEP_INTERVAL} seconds...")
        time.sleep(SLEEP_INTERVAL)
