import io

import pandas as pd
import numpy as np

from dotenv import load_dotenv
from functions.utils import save_df_to_s3, get_s3_client

load_dotenv()


def dim_customers(cust_info, cust_az12, loc_a101):
    dim_df = cust_info.merge(cust_az12, how="left",
                             left_on="cst_key", right_on="cid").merge(loc_a101, how="left", left_on="cst_key", right_on="cid")
    dim_df['customer_key'] = dim_df['cst_id'].rank(method='first').astype(int)
    dim_df['customer_id'] = dim_df['cst_id']
    dim_df['customer_number'] = dim_df['cst_key']
    dim_df['first_name'] = dim_df['cst_firstname']
    dim_df['last_name'] = dim_df['cst_lastname']
    dim_df['marital_status'] = dim_df['cst_marital_status']
    dim_df['country'] = dim_df['cntry']
    dim_df['gender'] = np.where(
        dim_df['cst_gndr'] != np.nan, dim_df['cst_gndr'], dim_df['gen'].fillna(np.nan))
    dim_df['birthdate'] = dim_df['bdate']
    dim_df['create_date'] = dim_df['cst_create_date']

    return dim_df[['customer_key', 'customer_id', 'customer_number', 'first_name', 'last_name', 'country', 'marital_status', 'gender', 'birthdate', 'create_date']]


def dim_products(prd_info, px_cat):
    prd_info = prd_info[prd_info['prd_end_dt'].isna()].copy()
    dim_df = prd_info.merge(px_cat, how="left",
                            left_on="cat_id", right_on="ID")
    dim_df = dim_df.sort_values(
        by=['prd_start_dt', 'prd_key']).reset_index(drop=True)
    dim_df['product_key'] = dim_df.index + 1
    dim_df = dim_df.rename(columns={
        'prd_id': 'product_id',
        'prd_key': 'product_number',
        'prd_nm': 'product_name',
        'prd_cost': 'cost',
        'prd_line': 'product_line',
        'prd_start_dt': 'start_date',
        'cat_id': 'category_id',
        'cat': 'category',
        'subcat': 'subcategory'
    })

    return dim_df[['product_key', 'product_id', 'product_number', 'product_name', 'category_id', 'category', 'subcategory', 'maintenance', 'cost', 'product_line', 'start_date']]


def fact_sales(sales_details, gold_dim_products, gold_dim_customers):
    dim_df = sales_details.merge(gold_dim_products, how="left", left_on="sls_prd_key", right_on="product_number").merge(
        gold_dim_customers, how="left", left_on="sls_cust_id", right_on="customer_id")

    dim_df = dim_df.rename(columns={
        'sls_ord_num': 'order_number',
        'product_key': 'product_key',
        'customer_key': 'customer_key',
        'sls_order_dt': 'order_date',
        'sls_ship_dt': 'shipping_date',
        'sls_due_dt': 'due_date',
        'sls_sales': 'sales_amount',
        'sls_quantity': 'quantity',
        'sls_price': 'price'
    })
    return dim_df[['order_number', 'product_key', 'customer_key', 'order_date', 'shipping_date', 'due_date', 'sales_amount', 'quantity', 'price']]


def data_aggreviation():
    s3 = get_s3_client()
    cust_info_response = s3.get_object(
        Bucket="business-analytics-df", Key="warehouse/silver/source_crm/cust_info.csv")
    prd_info_response = s3.get_object(
        Bucket="business-analytics-df", Key="warehouse/silver/source_crm/prd_info.csv")
    sales_details_info_response = s3.get_object(
        Bucket="business-analytics-df", Key="warehouse/silver/source_crm/sales_details.csv")

    cust_erp_response = s3.get_object(
        Bucket="business-analytics-df", Key="warehouse/silver/source_erp/CUST_AZ12.csv")
    loc_erp_response = s3.get_object(
        Bucket="business-analytics-df", Key="warehouse/silver/source_erp/LOC_A101.csv")
    cat_erp_response = s3.get_object(
        Bucket="business-analytics-df", Key="warehouse/silver/source_erp/PX_CAT_G1V2.csv")

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

    gold_dim_customers = dim_customers(cust_info, cust_erp, loc_erp)
    gold_dim_products = dim_products(prd_info, cat_erp)
    gold_fact_sales = fact_sales(
        sales_details_info, gold_dim_products, gold_dim_customers)

    save_df_to_s3(s3, gold_dim_customers, "business-analytics-df",
                  "warehouse/gold/gold_dim_customers.csv")
    save_df_to_s3(s3, gold_dim_products, "business-analytics-df",
                  "warehouse/gold/gold_dim_products.csv")
    save_df_to_s3(s3, gold_fact_sales, "business-analytics-df",
                  "warehouse/gold/gold_dim_sales_details.csv")
