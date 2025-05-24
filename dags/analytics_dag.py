from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator

from datetime import datetime, timedelta

with DAG(
    "analytics_warehouse_dag",
    description="A DAG to Pull the sales and business data into warehouse for analyzation",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 5, 1),
    catchup=False,
) as dag:
    analytics_bucket = "business.analytics"

    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket", bucket_name=analytics_bucket, aws_conn_id="aws_default"
    )

    cust_erp_to_s3 = LocalFilesystemToS3Operator(
        task_id="cust_erp_to_s3",
        filename="/opt/airflow/data/source_erp/CUST_AZ12.csv",
        dest_key="warehouse/bronze/CUST_AZ112.csv",
        dest_bucket=analytics_bucket,
        replace=True,
        aws_conn_id="aws_default",
    )

    loc_erp_to_s3 = LocalFilesystemToS3Operator(
        task_id="loc_erp_to_s3",
        filename="/opt/airflow/data/source_erp/LOC_A101.csv",
        dest_key="warehouse/bronze/LOC_A101.csv",
        dest_bucket=analytics_bucket,
        replace=True,
        aws_conn_id="aws_default",
    )

    px_cat_erp_to_s3 = LocalFilesystemToS3Operator(
        task_id="px_cat_erp_to_s3",
        filename="/opt/airflow/data/source_erp/PX_CAT_G1V2.csv",
        dest_key="warehouse/bronze/PX_CAT_G1V2.csv",
        dest_bucket=analytics_bucket,
        replace=True,
        aws_conn_id="aws_default",
    )

    join_first_group = EmptyOperator(task_id="join_first_group")

    cust_info_to_s3 = SqlToS3Operator(
        task_id="cust_info_to_s3",
        sql_conn_id="postgres_default",
        query="select * from business.cust_info",
        s3_bucket=analytics_bucket,
        s3_key="warehouse/bronze/cust_info.csv",
        aws_conn_id="aws_default",
        replace=True,
    )

    prd_info_to_s3 = SqlToS3Operator(
        task_id="prd_info_to_s3",
        sql_conn_id="postgres_default",
        query="select * from business.prd_info",
        s3_bucket=analytics_bucket,
        s3_key="warehouse/bronze/prd_info.csv",
        aws_conn_id="aws_default",
        replace=True,
    )

    sales_details_to_s3 = SqlToS3Operator(
        task_id="sales_details_to_s3",
        sql_conn_id="postgres_default",
        query="select * from business.sales_details",
        s3_bucket=analytics_bucket,
        s3_key="warehouse/bronze/sales_details.csv",
        aws_conn_id="aws_default",
        replace=True,
    )

    create_s3_bucket >> [cust_erp_to_s3, loc_erp_to_s3,
                         px_cat_erp_to_s3] >> join_first_group

    join_first_group >> [cust_info_to_s3, prd_info_to_s3, sales_details_to_s3]
