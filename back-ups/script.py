import os
import psycopg2
import time
import requests
import wget
import boto3
from botocore.client import Config

DB_CONNECTION = os.environ["DB_URL"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
S3_GATEWAY = "https://gateway.storjshare.io"
S3_UPLOAD_PATH = "/auto-dump"

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

def upload_to_s3(audio_file_path, title):
    print("Uploading to S3 bucket...")
    s3 = boto3.client(
        "s3",
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        endpoint_url=S3_GATEWAY,
        config=Config(s3={"addressing_style": "path"})
    )
    with open(audio_file_path, "rb") as audio_file:
        s3.upload_fileobj(audio_file, S3_BUCKET_NAME, f"{S3_UPLOAD_PATH}/{title}")
    print(f"Uploaded: {title}")

def process_records(records):
    print("Processing records...")
    for url, title in records:
        print(f"Downloading title: {title}")

        audio_file_path = wget.download(url)
        upload_to_s3(audio_file_path, title)
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
        time.sleep(300)
