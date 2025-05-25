import io
import boto3
import os

from dotenv import load_dotenv

load_dotenv()


def save_df_to_s3(s3_client, df, bucket, key):
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="text/csv"
    )


def get_s3_client():
    return boto3.client(
        service_name="s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        region_name="us-east-1",
    )
