import os
import boto3
import io

import pandas as pd
import numpy as np

from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


def cust_info_transform(cust_info):
    cust_info = cust_info[cust_info['cst_id'].notna()].copy()

    cust_info.loc[:, 'cst_firstname'] = cust_info['cst_firstname'].str.strip()
    cust_info.loc[:, 'cst_lastname'] = cust_info['cst_lastname'].str.strip()

    cust_info.loc[:, 'cst_marital_status'] = cust_info['cst_marital_status'].str.strip().str.upper().map({
        'S': 'Single',
        'M': 'Married'
    }).fillna(np.nan)

    cust_info.loc[:, 'cst_gndr'] = cust_info['cst_gndr'].str.strip().str.upper().map({
        'F': 'Female',
        'M': 'Male'
    }).fillna(np.nan)

    cust_info.loc[:, 'flag_last'] = cust_info.sort_values(
        'cst_create_date', ascending=False).groupby('cst_id').cumcount() + 1

    cust_info_latest = cust_info[cust_info['flag_last'] == 1]

    return cust_info_latest


def prd_info_transform(prd_info):
    prd_info = prd_info.copy()
    prd_info['cat_id'] = prd_info['prd_key'].str.slice(
        0, 5).str.replace('-', '_', regex=False)

    prd_info['prd_key'] = prd_info['prd_key'].str.slice(6)
    prd_info['prd_cost'] = prd_info['prd_cost'].fillna(0)

    prd_info['prd_line'] = prd_info['prd_line'].str.strip().str.upper().map({
        'M': 'Mountain',
        'R': 'Road',
        'S': 'Other Sales',
        'T': 'Touring'
    }).fillna(np.nan)

    prd_info['prd_start_dt'] = pd.to_datetime(prd_info['prd_start_dt'])
    prd_info = prd_info.sort_values(['prd_key', 'prd_start_dt'])

    prd_info['next_start_dt'] = prd_info.groupby(
        'prd_key')['prd_start_dt'].shift(-1)
    prd_info['prd_end_dt'] = prd_info['next_start_dt'] - pd.Timedelta(days=1)

    prd_info_latest = prd_info.drop(columns=['next_start_dt'])

    return prd_info_latest


def parse_date(val):
    if pd.isna(val) or len(str(int(val))) != 8 or int(val) == 0:
        return pd.NaT
    return pd.to_datetime(str(int(val)), format='%Y%m%d')


def sales_details_transform(sales_details):
    sales_details = sales_details.copy()

    sales_details['sls_order_dt'] = pd.to_numeric(
        sales_details['sls_order_dt'], errors='coerce')
    sales_details['sls_ship_dt'] = pd.to_numeric(
        sales_details['sls_ship_dt'], errors='coerce')
    sales_details['sls_due_dt'] = pd.to_numeric(
        sales_details['sls_due_dt'], errors='coerce')

    sales_details['sls_order_dt'] = sales_details['sls_order_dt'].apply(
        parse_date)
    sales_details['sls_ship_dt'] = sales_details['sls_ship_dt'].apply(
        parse_date)
    sales_details['sls_due_dt'] = sales_details['sls_due_dt'].apply(parse_date)

    sales_details['sls_quantity'] = pd.to_numeric(
        sales_details['sls_quantity'], errors='coerce')
    sales_details['sls_price'] = pd.to_numeric(
        sales_details['sls_price'], errors='coerce')
    sales_details['sls_sales'] = pd.to_numeric(
        sales_details['sls_sales'], errors='coerce')

    recomputed_sales = sales_details['sls_quantity'] * \
        sales_details['sls_price'].abs()
    invalid_sales = (
        sales_details['sls_sales'].isna() |
        (sales_details['sls_sales'] <= 0) |
        (sales_details['sls_sales'] != recomputed_sales)
    )
    sales_details.loc[invalid_sales,
                      'sls_sales'] = recomputed_sales[invalid_sales]

    invalid_price = sales_details['sls_price'].isna() | (
        sales_details['sls_price'] <= 0)
    safe_quantity = sales_details['sls_quantity'].replace(0, np.nan)
    derived_price = sales_details['sls_sales'] / safe_quantity
    sales_details.loc[invalid_price,
                      'sls_price'] = derived_price[invalid_price]

    return sales_details


def cust_transform(cust_df):
    cust_df = cust_df.copy()

    cust_df['cid'] = cust_df['CID'].astype(
        str).str.replace(r'^NAS', '', regex=True)

    cust_df['bdate'] = pd.to_datetime(cust_df['BDATE'], errors='coerce')
    cust_df['bdate'] = cust_df['bdate'].where(
        cust_df['bdate'] <= pd.Timestamp.today(), pd.NaT)
    cust_df['bdate'] = cust_df['bdate'].dt.normalize()

    cust_df['gen'] = cust_df['GEN'].astype(str).str.strip().str.upper()
    cust_df['gen'] = cust_df['gen'].map({
        'F': 'Female', 'FEMALE': 'Female',
        'M': 'Male', 'MALE': 'Male'
    }).fillna(np.nan)

    return cust_df[['cid', 'bdate', 'gen']]


def loc_transform(loc_df):
    loc_df = loc_df.copy()
    loc_df['cid'] = loc_df['CID'].astype(str).str.replace('-', '')

    # Normalize CNTRY values
    loc_df['cntry'] = loc_df['CNTRY'].astype(str).str.strip()
    loc_df['cntry'] = loc_df['cntry'].replace({
        'DE': 'Germany',
        'US': 'United States of America',
        'USA': 'United States of America',
        '': np.nan,
        'NAN': np.nan
    })

    # Select relevant columns
    return loc_df[['cid', 'cntry']]


def px_cat_transform(px_cat_df):
    px_cat_df = px_cat_df.copy()
    px_cat_df = px_cat_df.rename(columns={
        'CAT': 'cat',
        'SUBCAT': 'subcat',
        'MAINTENANCE': 'maintenance'
    })

    return px_cat_df


s3_client = boto3.client(
    service_name="s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    region_name="us-east-1",
)


def save_df_to_s3(df, bucket, key):
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="text/csv"
    )


def data_preprocessing_dim(**kwargs):
    cust_info_response = s3_client.get_object(
        Bucket="business.analytics", Key="warehouse/bronze/source_crm/cust_info.csv")
    prd_info_response = s3_client.get_object(
        Bucket="business.analytics", Key="warehouse/bronze/source_crm/prd_info.csv")
    sales_details_info_response = s3_client.get_object(
        Bucket="business.analytics", Key="warehouse/bronze/source_crm/sales_details.csv")

    cust_erp_response = s3_client.get_object(
        Bucket="business.analytics", Key="warehouse/bronze/source_erp/CUST_AZ12.csv")
    loc_erp_response = s3_client.get_object(
        Bucket="business.analytics", Key="warehouse/bronze/source_erp/LOC_A101.csv")
    cat_erp_response = s3_client.get_object(
        Bucket="business.analytics", Key="warehouse/bronze/source_erp/PX_CAT_G1V2.csv")

    cust_info = pd.read_csv(io.StringIO(
        cust_info_response["Body"].read().decode('utf-8')))
    prd_info = pd.read_csv(io.StringIO(
        prd_info_response["Body"].read().decode('utf-8')))
    sales_details_info = pd.read_csv(io.StringIO(
        sales_details_info_response["Body"].read().decode('utf-8')))

    cust_erp = pd.read_csv(io.StringIO(
        cust_erp_response["Body"].read().decode('utf-8')))
    loc_erp = pd.read_csv(io.StringIO(
        loc_erp_response["Body"].read().decode('utf-8')))
    cat_erp = pd.read_csv(io.StringIO(
        cat_erp_response["Body"].read().decode('utf-8')))

    cust_info_df = cust_info_transform(cust_info)
    prd_info_df = prd_info_transform(prd_info)
    sales_details_info_df = sales_details_transform(sales_details_info)

    cust_erp_df = cust_transform(cust_erp)
    loc_erp_df = loc_transform(loc_erp)
    cat_erp_df = px_cat_transform(cat_erp)

    save_df_to_s3(cust_info_df, "business.analytics",
                  "warehouse/silver/source_crm/cust_info.csv")
    save_df_to_s3(prd_info_df, "business.analytics",
                  "warehouse/silver/source_crm/prd_info.csv")
    save_df_to_s3(sales_details_info_df, "business.analytics",
                  "warehouse/silver/source_crm/sales_details.csv")

    save_df_to_s3(cust_erp_df, "business.analytics",
                  "warehouse/silver/source_erp/CUST_AZ12.csv")
    save_df_to_s3(loc_erp_df, "business.analytics",
                  "warehouse/silver/source_erp/LOC_A101.csv")
    save_df_to_s3(cat_erp_df, "business.analytics",
                  "warehouse/silver/source_erp/PX_CAT_G1V2.csv")
