import duckdb
import os
from functions.utils import get_s3_client


def data_aggregate():
    s3 = get_s3_client()
    duckdb_path = '/opt/airflow/data/warehouse/business_analytics.db'

    # Ensure directory exists
    os.makedirs(os.path.dirname(duckdb_path), exist_ok=True)

    try:
        # Download database from S3
        print("Downloading database from S3...")
        s3.download_file(
            Filename=duckdb_path,
            Bucket="business-analytics-sql",
            Key="warehouse/business_analytics.db",
        )
        print("Database downloaded successfully")
    except Exception as e:
        print(f"Error downloading database: {e}")
        raise

    # Use context manager for proper connection handling
    try:
        with duckdb.connect(database=duckdb_path) as con:
            print("Connected to DuckDB database")

            # Ensure gold schema exists
            con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

            # Drop existing views if they exist
            gold_views = ['gold.dim_customers',
                          'gold.dim_products', 'gold.fact_sales']

            print("Dropping existing gold views...")
            for view in gold_views:
                try:
                    con.execute(f"DROP VIEW IF EXISTS {view};")
                    print(f"Dropped view: {view}")
                except Exception as e:
                    print(f"Warning: Could not drop view {view}: {e}")

            # Create dimensional views with error handling
            view_definitions = [
                ("Customer Dimension", """
                    CREATE VIEW gold.dim_customers AS
                    SELECT
                        ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
                        ci.cst_id                          AS customer_id,
                        ci.cst_key                         AS customer_number,
                        ci.cst_firstname                   AS first_name,
                        ci.cst_lastname                    AS last_name,
                        COALESCE(la.cntry, 'n/a')          AS country,
                        ci.cst_marital_status              AS marital_status,
                        CASE 
                            WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
                            ELSE COALESCE(ca.gen, 'n/a')               -- Fallback to ERP data
                        END                                AS gender,
                        ca.bdate                           AS birthdate,
                        ci.cst_create_date                 AS create_date,
                        ci.dwh_create_date                 AS dwh_create_date
                    FROM silver.crm_cust_info ci
                    LEFT JOIN silver.erp_cust ca
                        ON ci.cst_key = ca.cid
                    LEFT JOIN silver.erp_loc la
                        ON ci.cst_key = la.cid
                """),

                ("Product Dimension", """
                    CREATE VIEW gold.dim_products AS
                    SELECT
                        ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
                        pn.prd_id                          AS product_id,
                        pn.prd_key                         AS product_number,
                        pn.prd_nm                          AS product_name,
                        pn.cat_id                          AS category_id,
                        COALESCE(pc.cat, 'n/a')            AS category,
                        COALESCE(pc.subcat, 'n/a')         AS subcategory,
                        COALESCE(pc.maintenance, 'n/a')    AS maintenance,
                        pn.prd_cost                        AS cost,
                        pn.prd_line                        AS product_line,
                        pn.prd_start_dt                    AS start_date,
                        pn.prd_end_dt                      AS end_date,
                        pn.dwh_create_date                 AS dwh_create_date
                    FROM silver.crm_prd_info pn
                    LEFT JOIN silver.erp_px_cat pc 
                        ON pn.cat_id = pc.id
                    WHERE pn.prd_end_dt IS NULL OR pn.prd_end_dt >= CURRENT_DATE
                """),

                ("Sales Fact Table", """
                    CREATE VIEW gold.fact_sales AS
                    SELECT
                        sd.sls_ord_num                     AS order_number,
                        COALESCE(pr.product_key, -1)       AS product_key,    -- -1 for unknown products
                        COALESCE(cu.customer_key, -1)      AS customer_key,   -- -1 for unknown customers
                        sd.sls_order_dt                    AS order_date,
                        sd.sls_ship_dt                     AS shipping_date,
                        sd.sls_due_dt                      AS due_date,
                        sd.sls_sales                       AS sales_amount,
                        sd.sls_quantity                    AS quantity,
                        sd.sls_price                       AS price,
                        sd.dwh_create_date                 AS dwh_create_date
                    FROM silver.crm_sales_details sd
                    LEFT JOIN gold.dim_products pr
                        ON sd.sls_prd_key = pr.product_number
                    LEFT JOIN gold.dim_customers cu
                        ON sd.sls_cust_id = cu.customer_id
                    WHERE sd.sls_order_dt IS NOT NULL  -- Filter out records without order dates
                """)
            ]

            print("Creating gold dimensional views...")
            for i, (name, sql) in enumerate(view_definitions, 1):
                try:
                    print(f"Creating {name}...")
                    con.execute(sql)
                    print(
                        f"✓ {name} created successfully ({i}/{len(view_definitions)})")
                except Exception as e:
                    print(f"✗ Error creating {name}: {e}")
                    raise

            # Verify views were created and contain data
            print("\nVerifying dimensional views:")
            verification_queries = [
                ("Customer Dimension", "SELECT COUNT(*) FROM gold.dim_customers"),
                ("Product Dimension", "SELECT COUNT(*) FROM gold.dim_products"),
                ("Sales Fact", "SELECT COUNT(*) FROM gold.fact_sales")
            ]

            for view_name, query in verification_queries:
                try:
                    result = con.execute(query).fetchone()
                    count = result[0] if result else 0
                    print(f"  {view_name}: {count} rows")
                except Exception as e:
                    print(f"  {view_name}: Error counting rows - {e}")

            # Additional data quality checks
            print("\nPerforming data quality checks:")

            quality_checks = [
                ("Orphaned Sales (no customer)", """
                    SELECT COUNT(*) 
                    FROM gold.fact_sales 
                    WHERE customer_key = -1
                """),
                ("Orphaned Sales (no product)", """
                    SELECT COUNT(*) 
                    FROM gold.fact_sales 
                    WHERE product_key = -1
                """),
                ("Sales with null amounts", """
                    SELECT COUNT(*) 
                    FROM gold.fact_sales 
                    WHERE sales_amount IS NULL OR sales_amount <= 0
                """),
                ("Customers without country", """
                    SELECT COUNT(*) 
                    FROM gold.dim_customers 
                    WHERE country = 'n/a'
                """)
            ]

            for check_name, query in quality_checks:
                try:
                    result = con.execute(query).fetchone()
                    count = result[0] if result else 0
                    status = "⚠️" if count > 0 else "✓"
                    print(f"  {status} {check_name}: {count}")
                except Exception as e:
                    print(f"  ❌ {check_name}: Error - {e}")

            print("Gold layer views created successfully")

    except Exception as e:
        print(f"Error during gold layer creation: {e}")
        raise

    try:
        # Upload updated database back to S3
        print("Uploading updated database to S3...")
        s3.upload_file(
            Filename=duckdb_path,
            Bucket="business-analytics-sql",
            Key="warehouse/business_analytics.db"
        )
        print("Database uploaded successfully")

        # Clean up local file
        if os.path.exists(duckdb_path):
            os.remove(duckdb_path)
            print("Local database file cleaned up")

    except Exception as e:
        print(f"Error uploading database: {e}")
        raise

    print("Data aggregation completed successfully!")
