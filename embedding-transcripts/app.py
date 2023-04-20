import os
import psycopg2
from psycopg2 import sql
import openai
import time

# Read environment variables
DB_URL = os.getenv("DB_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Set up OpenAI API
openai.api_key = OPENAI_API_KEY

def update_embeddings():
    # Connect to the database
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    # Read the 50 rows with the highest id from table "terra_audio"
    cursor.execute("SELECT * FROM terra_audio ORDER BY id DESC LIMIT 50;")
    rows = cursor.fetchall()
    print(f"Number of new lines found: {len(rows)}")

    # Get all table names in the database
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
    existing_tables = [t[0] for t in cursor.fetchall()]

    # Initialize a set to store processed tables in the current run
    processed_tables = set()

    # Process each row
    for row in rows:
        title = row[1]
        transcript = row[5]
        table_name = title.replace(" ", "_")[:63]  # Truncate the table name to a maximum of 63 characters

        if transcript is None:
            print(f"Skipping row with empty transcript: '{title}'")
            continue

        if table_name not in existing_tables and table_name != "terra_audio" and table_name not in processed_tables:
            # Table does not exist, is not "terra_audio", and has not been processed yet, create it and store embeddings
            print(f"New table created: '{table_name}'")
            create_and_store_embeddings(cursor, conn, table_name, transcript)
            existing_tables.append(table_name)  # Add the created table to the list of existing tables
            processed_tables.add(table_name)  # Add the table to the set of processed tables
        elif table_name != "terra_audio":
            # Table exists and is not "terra_audio", check if title has a corresponding row
            cursor.execute(
                sql.SQL("SELECT COUNT(*) FROM {}").format(
                    sql.Identifier(table_name)
                )
            )
            count = cursor.fetchone()[0]

            if count == 0:
                # No corresponding row, delete table
                print(f"Deleting table: '{table_name}'")
                cursor.execute(
                    sql.SQL("DROP TABLE {}").format(
                        sql.Identifier(table_name)
                    )
                )
                conn.commit()

    if len(rows) == 0:
        print("No new entries found")

    # Close the connection
    cursor.close()
    conn.close()

def create_and_store_embeddings(cursor, conn, table_name, transcript):
    # Create the table with columns for "id", "embedding", "text"
    cursor.execute(
        sql.SQL("CREATE TABLE {} (id SERIAL PRIMARY KEY, embedding float[], text text);").format(
            sql.Identifier(table_name)
        )
    )
    conn.commit()

    # Process the transcript in chunks of 5 lines
    lines = transcript.splitlines()
    for i in range(0, len(lines), 5):
        chunk = "\n".join(lines[i:i+5])

        # Call OpenAI API to get the embeddings
        response = openai.Embedding.create(
            model="text-embedding-ada-002",
            input=chunk
        )
        embedding = response["data"][0]["embedding"]

        # Store the output from OpenAI embedding API call in the new table
        cursor.execute(
            sql.SQL("INSERT INTO {} (embedding, text) VALUES (%s, %s);").format(
                sql.Identifier(table_name)
            ),
            (embedding, chunk)
        )
        conn.commit()

    print(f"Embeddings stored in the new table '{table_name}'")
    print("Embedding finished")

# Run the update_embeddings function every 10 minutes
while True:
    update_embeddings()
    time.sleep(600)  # 10
