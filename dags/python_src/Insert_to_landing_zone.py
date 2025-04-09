import psycopg2
import boto3
import os
import pandas as pd
from io import StringIO
import logging
from botocore.exceptions import ClientError
from datetime import datetime

def postgres_sink(ds, **kwargs):
    s3_client = boto3.client('s3',
                        endpoint_url=os.environ.get("MINIO_ENDPOINT"),
                        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY"),
                        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY")
                        )

    bucket_name='reddit-data'
    date = datetime.strptime(ds, "%Y-%m-%d")
    object_key = f"{date.strftime("%Y-%m-%d")}/reddit_data.csv"

    try:
            s3_client.head_object(Bucket=bucket_name, Key=object_key)
            logging.info(f"Object '{object_key}' exists in bucket '{bucket_name}'.")

        
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
                logging.error(f"Object '{object_key}' does NOT exist in bucket '{bucket_name}'.")         
        else:
                logging.error(f"Error checking object: {e}")

    response = s3_client.get_object(Bucket='reddit-data', Key = object_key)
    # Process or use the content as needed
    file_content = response['Body'].read().decode("utf-8")
    posts_df = pd.read_csv(StringIO(file_content)) #stringIo is used to treat a string as a file

    try:
            conn = psycopg2.connect(database=os.environ.get("DB"), 
                                    user=os.environ.get("DB_USER"), 
                                    password=os.environ.get("DB_PWD"), 
                                    host=os.environ.get("HOST"), 
                                    port=os.environ.get("PGDB_PORT"))
            
            # Open a cursor to perform database operations
            cur = conn.cursor()
            for _, row in posts_df.iterrows():
                cur.execute("""INSERT INTO raw_reddit_posts (id, title, body, category, author, author_id, 
                    author_createdAt, post_created_utc, upvotes, num_comments, url, comments)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING""", tuple(row))
            
            # Commit and close
            conn.commit()
            cur.close()
            conn.close()
            logging.info("Data successfully inserted into PostgreSQL")

    except:
            logging.error("Could not connect to postgress")
            raise