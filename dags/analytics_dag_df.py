from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta

from functions.df.silver_medallion import data_preprocessing_dim
from functions.df.gold_medallion import data_aggreviation

with DAG(
    "analytics_warehouse_df_dag",
    description="A DAG to Pull the sales and business data df into warehouse for analyzation",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 5, 1),
    tags=['data_warehouse', 'dataframe'],
    catchup=False,
) as dag:
    analytics_bucket = "business-analytics-df"

    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket", bucket_name=analytics_bucket, aws_conn_id="aws_default"
    )

    bronze_medallion = EmptyOperator(task_id="bronze_medallion_init")

    with TaskGroup("local_filesystem_to_s3_group", tooltip="Business data from local to S3") as local_to_s3_group:

        cust_info_to_s3 = LocalFilesystemToS3Operator(
            task_id="cust_info_to_s3",
            filename="/opt/airflow/data/source_crm/cust_info.csv",
            dest_key="warehouse/bronze/source_crm/cust_info.csv",
            dest_bucket=analytics_bucket,
            replace=True,
            aws_conn_id="aws_default",
        )

        prd_info_to_s3 = LocalFilesystemToS3Operator(
            task_id="prd_info_to_s3",
            filename="/opt/airflow/data/source_crm/prd_info.csv",
            dest_key="warehouse/bronze/source_crm/prd_info.csv",
            dest_bucket=analytics_bucket,
            replace=True,
            aws_conn_id="aws_default",
        )

        sales_details_to_s3 = LocalFilesystemToS3Operator(
            task_id="sales_details_to_s3",
            filename="/opt/airflow/data/source_crm/sales_details.csv",
            dest_key="warehouse/bronze/source_crm/sales_details.csv",
            dest_bucket=analytics_bucket,
            replace=True,
            aws_conn_id="aws_default",
        )

        cust_erp_to_s3 = LocalFilesystemToS3Operator(
            task_id="cust_erp_to_s3",
            filename="/opt/airflow/data/source_erp/CUST_AZ12.csv",
            dest_key="warehouse/bronze/source_erp/CUST_AZ12.csv",
            dest_bucket=analytics_bucket,
            replace=True,
            aws_conn_id="aws_default",
        )

        loc_erp_to_s3 = LocalFilesystemToS3Operator(
            task_id="loc_erp_to_s3",
            filename="/opt/airflow/data/source_erp/LOC_A101.csv",
            dest_key="warehouse/bronze/source_erp/LOC_A101.csv",
            dest_bucket=analytics_bucket,
            replace=True,
            aws_conn_id="aws_default",
        )

        px_cat_erp_to_s3 = LocalFilesystemToS3Operator(
            task_id="px_cat_erp_to_s3",
            filename="/opt/airflow/data/source_erp/PX_CAT_G1V2.csv",
            dest_key="warehouse/bronze/source_erp/PX_CAT_G1V2.csv",
            dest_bucket=analytics_bucket,
            replace=True,
            aws_conn_id="aws_default",
        )

        cust_info_to_s3 >> prd_info_to_s3 >> sales_details_to_s3 >> cust_erp_to_s3 >> loc_erp_to_s3 >> px_cat_erp_to_s3

    silver_medallion = EmptyOperator(task_id="silver_medallion_init")

    data_preprocessing_task = PythonOperator(
        task_id='data_preprocessing_dim',
        python_callable=data_preprocessing_dim
    )

    gold_medallion = EmptyOperator(task_id="gold_medallion_init")

    data_aggrivate = PythonOperator(
        task_id='data_aggreviation',
        python_callable=data_aggreviation
    )

    create_s3_bucket >> bronze_medallion >> local_to_s3_group >> silver_medallion >> data_preprocessing_task >> gold_medallion >> data_aggrivate
