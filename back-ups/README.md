### In testing


docker run -t -d \
 -e DB_URL=<your_database_url> \
 -e S3_ACCESS_KEY=<your_s3_access_key> \
 -e S3_SECRET_KEY=<your_s3_secret_key> \
 -e S3_BUCKET_NAME=<your_s3_bucket_name> \
 image-name