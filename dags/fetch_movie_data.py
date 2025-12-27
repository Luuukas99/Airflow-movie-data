from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import os
import json
import psycopg2
import logging

API_KEY = '4c34c29bf0fddf33207a1ef7af994a06'
SAVE_DIR = '/opt/airflow/data'

DB_CONFIG = {
    'dbname': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'postgres'
}

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['matiusovas.lukas@gmail.com'],  # Add your email for failure alerts
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    dag_id='fetch_popular_media_taskflow',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:
    
    @task()
    def upload_to_s3(media_type: str, filename: str):
        logging.info(f"Uploading {filename} to S3 bucket")
        s3 = S3Hook(aws_conn_id='aws_default')  # Use the connection ID you configured in Airflow
        bucket_name = 'my-move-data-bucket'     # Replace with your actual bucket name
        key = f"tmdb/{media_type}/{os.path.basename(filename)}"
        s3.load_file(filename=filename, bucket_name=bucket_name, key=key, replace=True)
        logging.info(f"Uploaded {filename} to s3://{bucket_name}/{key}")

    @task()
    def fetch_tmdb_data(media_type: str) -> str:
        logging.info(f"Fetching popular {media_type}s from TMDB API")
        url = f'https://api.themoviedb.org/3/{media_type}/popular?api_key={API_KEY}&language=en-US&page=1'
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        os.makedirs(SAVE_DIR, exist_ok=True)
        filename = f"{SAVE_DIR}/{media_type}_popular_{datetime.now().strftime('%Y%m%d')}.json"
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        logging.info(f"Saved {media_type} data to {filename}")
        return filename

    @task()
    def load_tmdb_data(media_type: str, filename: str):
        logging.info(f"Loading {media_type} data from {filename} into DB")
        with open(filename) as f:
            data = json.load(f)

        items = data.get('results', [])
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        for item in items:
            cur.execute("""
                INSERT INTO media (id, title, release_date, popularity, vote_average, vote_count, media_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                item['id'],
                item.get('title') or item.get('name'),
                item.get('release_date') or item.get('first_air_date'),
                item['popularity'],
                item['vote_average'],
                item['vote_count'],
                media_type
            ))
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Loaded {len(items)} {media_type}s into the database")

    # Fetch and load movies
    movie_file = fetch_tmdb_data('movie')
    load_tmdb_data('movie', movie_file)
    upload_to_s3('movie', movie_file)

    # Fetch and load TV series
    tv_file = fetch_tmdb_data('tv')
    load_tmdb_data('tv', tv_file)
    upload_to_s3('tv', tv_file)