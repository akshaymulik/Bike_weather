# Bike_weather
Data pipelines
A. API to Bucket via Pub/Sub (Implemented on Docker and Airflow V2)
  0. Pub/Sub helps in concurrent asynchronous operations and storing data in bucket helps manage failures when we ingest to data lakes.
  1. Fetch Data from API and push to Pub/Sub service in Google Cloud
  2. Read from topics under Pub/Sub Subscription and store them in Cloud Bucket.
B. Read from Bucket to Big Querry
  0. A cloud Function will trigger the Airflow DAG with a context
  1. Airflow DAG will extract file name and bucket name to ingest the data in GCS for further processing.
