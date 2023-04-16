#### Docker Run

```
docker run -t -d \
  -e DB_URL=postgresql://USER:PASS@IP:PORT/DB_NAME?sslmode=prefer \
  --name terra-rss-pull \
  taoi33/rss-reader:latest
```

#### sacple command for makeing the table. "id" column needs to be set as primary key.
```
CREATE TABLE your_table_name (
    id SERIAL PRIMARY KEY,
    -- Add other columns here, separated by commas
    column1 data_type,
    column2 data_type
);

```
