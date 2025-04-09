from dotenv import load_dotenv
import os
import boto3
from datetime import datetime
     
def upload_to_minio(ds, **kwargs):
    
    print(os.environ.get("MINIO_ENDPOINT"))

    s3 = boto3.client('s3',
                    endpoint_url=os.environ.get("MINIO_ENDPOINT"),
                    aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY"),
                    aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY")
                    )

    
    try:
        # List buckets to verify connection
        response = s3.list_buckets()
        print("✅ Connection successful! Buckets:", [bucket["Name"] for bucket in response["Buckets"]])
    except Exception as e:
        print("❌ Connection failed:", str(e))
    
    date = datetime.strptime(ds, "%Y-%m-%d")
    file_path ='/opt/airflow/dags/python_src/output/scrapped_data1.csv'
    object_name = f"{date.strftime("%Y-%m-%d")}/reddit_data.csv" # gives current date

    try:
            s3.upload_file(file_path, 'reddit-data', object_name)
            print(f"Uploaded {file_path} to MinIO as {object_name}")
    except Exception as e:
            print(f"Error uploading to MinIO: {e}")
            raise