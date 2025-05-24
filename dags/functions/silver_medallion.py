import os
import boto3
import io

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

s3_client = boto3.client(
    service_name="s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    region_name="us-east-1",
)
