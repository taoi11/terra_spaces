#### Docker Run

```
docker run -t -d \
  -e DB_URL=postgresql://USER:PASS@IP:PORT/DB_NAME?sslmode=prefer \
  --name terra-rss-pull \
  taoi33/rss-reader:latest
```
