import duckdb
import os
from dotenv import load_dotenv
from functions.utils import get_s3_client

load_dotenv()


def data_ingestion():
    s3 = get_s3_client()
    duckdb_path = '/opt/airflow/data/warehouse/business_analytics.db'
    os.makedirs(os.path.dirname(duckdb_path), exist_ok=True)

    try:
        if os.path.exists(duckdb_path):
            os.remove(duckdb_path)
            print("Removed existing database file to clear locks")
    except Exception as e:
        print(f"Could not remove existing database: {e}")

    with duckdb.connect(database=duckdb_path) as con:
        con.execute(f"""
            SET s3_region='us-east-1';
            SET s3_endpoint='minio:9000';
            SET s3_access_key_id='{os.getenv("MINIO_ACCESS_KEY")}';
            SET s3_secret_access_key='{os.getenv("MINIO_SECRET_KEY")}';
            SET s3_use_ssl='false';
            SET s3_url_style='path';
        """)

        csv_files = {
            "cust_info": 's3://business-analytics-sql/warehouse/data_raw/source_crm/cust_info.csv',
            "prd_info": 's3://business-analytics-sql/warehouse/data_raw/source_crm/prd_info.csv',
            "sales_details_info": 's3://business-analytics-sql/warehouse/data_raw/source_crm/sales_details.csv',
            "cust_erp": 's3://business-analytics-sql/warehouse/data_raw/source_erp/CUST_AZ12.csv',
            "loc_erp": 's3://business-analytics-sql/warehouse/data_raw/source_erp/LOC_A101.csv',
            "cat_erp": 's3://business-analytics-sql/warehouse/data_raw/source_erp/PX_CAT_G1V2.csv'
        }

        # Create schemas if not exist
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
        con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

        # Add error handling for table creation
        for table, path in csv_files.items():
            try:
                print(f"Processing table: {table}")
                con.execute(f"DROP TABLE IF EXISTS bronze.{table};")
                con.execute(f"""
                    CREATE TABLE bronze.{table} AS
                    SELECT * FROM read_csv_auto('{path}');
                """)
                print(f"Successfully created table: bronze.{table}")
            except Exception as e:
                print(f"Error creating table {table}: {str(e)}")
                raise

        con.close()

    # Upload to S3
    s3.upload_file(
        Filename=duckdb_path,
        Bucket="business-analytics-sql",
        Key="warehouse/business_analytics.db"
    )
    print("Database uploaded to S3 successfully")
