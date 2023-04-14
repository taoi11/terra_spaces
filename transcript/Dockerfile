# Use the official Python image as the base image
FROM python:3.9

# Set the working directory
WORKDIR /app

# Install required libraries
RUN pip install --no-cache-dir psycopg2-binary wget requests

# Set the environment variables for the database and transcribe API URLs
ENV DB_URL=your_database_url
ENV TRANSCRIBE_API_URL=your_transcribe_api_url

# Copy the script into the container
COPY script.py .

# Run the script
CMD ["python", "script.py"]


# docker build -t terra .
# docker run --name whisper-test terra
