FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

ENV DB_URL=your_database_url
ENV TRANSCRIBE_API_URL=your_transcribe_api_url

COPY script.py .

CMD ["python", "script.py"]

