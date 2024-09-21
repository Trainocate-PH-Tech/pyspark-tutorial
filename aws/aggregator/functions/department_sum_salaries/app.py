import json
import os
import boto3
from pyspark.sql import SparkSession

def lambda_handler(event, context):
    bucket  = event.get("Records")[0].get("s3").get("bucket").get("name")
    key     = event.get("Records")[0].get("s3").get("object").get("key")

    print(f"Bucket: {bucket}")
    print(f"Key: {key}")

    s3_client = boto3.client('s3')
    local_file_path = '/tmp/data.csv'

    try:
        print(f"Downloading file {key} to {local_file_path}")

        s3_client.download_file(bucket, key, local_file_path)

        # Get the file size in bytes
        file_size_bytes = os.path.getsize(local_file_path)

        # Convert to gigabytes
        file_size_gb = file_size_bytes / (1024 ** 3)

        print(f"Successfully download file {key} with size {file_size_gb} GB")

        return { "statusCode": 200,
            "body": json.dumps({
                "message": "done"
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error: {e}"
        }
