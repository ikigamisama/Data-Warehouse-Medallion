from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta


with DAG(
    "analytics_warehouse_sql_dag",
    description="A DAG to Pull the sales and business data SQL into warehouse for analyzation",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 5, 1),
    catchup=False,
) as dag:

    analytics_bucket = "business.analytics"

    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket", bucket_name=analytics_bucket, aws_conn_id="aws_default"
    )

    bronze_medallion = EmptyOperator(task_id="bronze_medallion_init")

    with TaskGroup("sql_to_s3_group", tooltip="Business SQL data to S3") as sql_to_s3_group:
        cust_info_to_s3 = SqlToS3Operator(
            task_id="cust_info_to_s3",
            sql_conn_id="postgres_default",
            query="SELECT * FROM business.cust_info",
            s3_bucket=analytics_bucket,
            s3_key="warehouse/bronze/cust_info.csv",
            aws_conn_id="aws_default",
            replace=True,
        )

        prd_info_to_s3 = SqlToS3Operator(
            task_id="prd_info_to_s3",
            sql_conn_id="postgres_default",
            query="SELECT * FROM business.prd_info",
            s3_bucket=analytics_bucket,
            s3_key="warehouse/bronze/prd_info.csv",
            aws_conn_id="aws_default",
            replace=True,
        )

        sales_details_to_s3 = SqlToS3Operator(
            task_id="sales_details_to_s3",
            sql_conn_id="postgres_default",
            query="SELECT * FROM business.sales_details",
            s3_bucket=analytics_bucket,
            s3_key="warehouse/bronze/sales_details.csv",
            aws_conn_id="aws_default",
            replace=True,
        )

        cust_info_to_s3 >> prd_info_to_s3 >> sales_details_to_s3
