
```
docker run -t -d \
  -e DB_URL=postgresql://USER:PASS@IP:PORT/DB_NAME?sslmode=prefer \
  -e TRANSCRIBE_API_URL=http://IP:PORT \
  -v /transcripts/location/:/output \
  --name terra_whisper \
  taoi33/transcript:1
```
