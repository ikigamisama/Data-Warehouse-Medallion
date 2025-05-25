from airflow import DAG

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta

from functions.sql.bronze_transform import data_ingestion
from functions.sql.silver_transform import data_preprocessing
from functions.sql.gold_transform import data_aggregate


with DAG(
    "analytics_warehouse_sql_dag",
    description="A DAG to Pull the sales and business data SQL into warehouse for analyzation",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 5, 1),
    tags=['data_warehouse', 'sql'],
    catchup=False,
) as dag:
    analytics_bucket = "business-analytics-sql"

    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket", bucket_name=analytics_bucket, aws_conn_id="aws_default"
    )

    with TaskGroup("local_filesystem_to_s3_group", tooltip="Business data from local to S3") as local_to_s3_group:
        cust_info_to_s3 = LocalFilesystemToS3Operator(
            task_id="cust_info_to_s3",
            filename="/opt/airflow/data/source_crm/cust_info.csv",
            dest_key="warehouse/data_raw/source_crm/cust_info.csv",
            dest_bucket=analytics_bucket,
            replace=True,
            aws_conn_id="aws_default",
        )

        prd_info_to_s3 = LocalFilesystemToS3Operator(
            task_id="prd_info_to_s3",
            filename="/opt/airflow/data/source_crm/prd_info.csv",
            dest_key="warehouse/data_raw/source_crm/prd_info.csv",
            dest_bucket=analytics_bucket,
            replace=True,
            aws_conn_id="aws_default",
        )

        sales_details_to_s3 = LocalFilesystemToS3Operator(
            task_id="sales_details_to_s3",
            filename="/opt/airflow/data/source_crm/sales_details.csv",
            dest_key="warehouse/data_raw/source_crm/sales_details.csv",
            dest_bucket=analytics_bucket,
            replace=True,
            aws_conn_id="aws_default",
        )

        cust_erp_to_s3 = LocalFilesystemToS3Operator(
            task_id="cust_erp_to_s3",
            filename="/opt/airflow/data/source_erp/CUST_AZ12.csv",
            dest_key="warehouse/data_raw/source_erp/CUST_AZ12.csv",
            dest_bucket=analytics_bucket,
            replace=True,
            aws_conn_id="aws_default",
        )

        loc_erp_to_s3 = LocalFilesystemToS3Operator(
            task_id="loc_erp_to_s3",
            filename="/opt/airflow/data/source_erp/LOC_A101.csv",
            dest_key="warehouse/data_raw/source_erp/LOC_A101.csv",
            dest_bucket=analytics_bucket,
            replace=True,
            aws_conn_id="aws_default",
        )

        px_cat_erp_to_s3 = LocalFilesystemToS3Operator(
            task_id="px_cat_erp_to_s3",
            filename="/opt/airflow/data/source_erp/PX_CAT_G1V2.csv",
            dest_key="warehouse/data_raw/source_erp/PX_CAT_G1V2.csv",
            dest_bucket=analytics_bucket,
            replace=True,
            aws_conn_id="aws_default",
        )

    bronze_medallion = EmptyOperator(task_id="bronze_medallion_init")

    bronze_data_ingestion = PythonOperator(
        task_id='data_ingestion',
        python_callable=data_ingestion
    )

    silver_medallion = EmptyOperator(task_id="silver_medallion_init")

    silver_data_preprocessing = PythonOperator(
        task_id='data_preprocessing',
        python_callable=data_preprocessing
    )

    gold_medallion = EmptyOperator(task_id="gold_medallion_init")

    gold_data_aggregate = PythonOperator(
        task_id='gold_data_aggregate',
        python_callable=data_aggregate
    )

    create_s3_bucket >> local_to_s3_group >> bronze_medallion
    bronze_medallion >> bronze_data_ingestion >> silver_medallion
    silver_medallion >> silver_data_preprocessing >> gold_medallion
    gold_medallion >> gold_data_aggregate
