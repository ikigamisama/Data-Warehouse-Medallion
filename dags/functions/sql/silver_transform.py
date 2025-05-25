import duckdb
import os
from functions.utils import get_s3_client


def data_preprocessing():
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

            # Drop existing silver tables
            silver_tables = [
                'silver.crm_cust_info',
                'silver.crm_prd_info',
                'silver.crm_sales_details',
                'silver.erp_loc',
                'silver.erp_cust',
                'silver.erp_px_cat'
            ]

            print("Dropping existing silver tables...")
            for table in silver_tables:
                con.execute(f"DROP TABLE IF EXISTS {table};")

            # Create silver schema if not exists
            con.execute("CREATE SCHEMA IF NOT EXISTS silver;")

            create_statements = [
                """
                CREATE TABLE silver.crm_cust_info (
                    cst_id             INT,
                    cst_key            VARCHAR(50),
                    cst_firstname      VARCHAR(50),
                    cst_lastname       VARCHAR(50),
                    cst_marital_status VARCHAR(50),
                    cst_gndr           VARCHAR(50),
                    cst_create_date    DATE,
                    dwh_create_date    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,

                """
                CREATE TABLE silver.crm_prd_info (
                    prd_id          INT,
                    cat_id          VARCHAR(50),
                    prd_key         VARCHAR(50),
                    prd_nm          VARCHAR(50),
                    prd_cost        INT,
                    prd_line        VARCHAR(50),
                    prd_start_dt    DATE,
                    prd_end_dt      DATE,
                    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,

                """
                CREATE TABLE silver.crm_sales_details (
                    sls_ord_num     VARCHAR(50),
                    sls_prd_key     VARCHAR(50),
                    sls_cust_id     INT,
                    sls_order_dt    DATE,
                    sls_ship_dt     DATE,
                    sls_due_dt      DATE,
                    sls_sales       INT,
                    sls_quantity    INT,
                    sls_price       INT,
                    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,

                """
                CREATE TABLE silver.erp_loc (
                    cid             VARCHAR(50),
                    cntry           VARCHAR(50),
                    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,

                """
                CREATE TABLE silver.erp_cust (
                    cid             VARCHAR(50),
                    bdate           DATE,
                    gen             VARCHAR(50),
                    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,

                """
                CREATE TABLE silver.erp_px_cat (
                    id              VARCHAR(50),
                    cat             VARCHAR(50),
                    subcat          VARCHAR(50),
                    maintenance     VARCHAR(50),
                    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            ]

            print("Creating silver table schemas...")
            for i, sql in enumerate(create_statements, 1):
                try:
                    con.execute(sql)
                    print(f"Created table schema {i}/{len(create_statements)}")
                except Exception as e:
                    print(f"Error creating table schema {i}: {e}")
                    raise

            # Data transformation and loading with better error handling
            transformations = [
                ("CRM Customer Info", """
                    INSERT INTO silver.crm_cust_info (
                        cst_id, 
                        cst_key, 
                        cst_firstname, 
                        cst_lastname, 
                        cst_marital_status, 
                        cst_gndr,
                        cst_create_date
                    )
                    SELECT
                        cst_id,
                        cst_key,
                        TRIM(cst_firstname) AS cst_firstname,
                        TRIM(cst_lastname) AS cst_lastname,
                        CASE 
                            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                            ELSE 'n/a'
                        END AS cst_marital_status,
                        CASE 
                            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                            ELSE 'n/a'
                        END AS cst_gndr,
                        cst_create_date
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
                        FROM bronze.cust_info
                        WHERE cst_id IS NOT NULL
                    ) t
                    WHERE flag_last = 1
                """),

                ("CRM Product Info", """
                    INSERT INTO silver.crm_prd_info (
                        prd_id,
                        cat_id,
                        prd_key,
                        prd_nm,
                        prd_cost,
                        prd_line,
                        prd_start_dt,
                        prd_end_dt
                    )
                    SELECT
                        prd_id,
                        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
                        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
                        prd_nm,
                        COALESCE(prd_cost, 0) AS prd_cost,
                        CASE 
                            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                            ELSE 'n/a'
                        END AS prd_line,
                        CAST(prd_start_dt AS DATE) AS prd_start_dt,
                        CAST(
                            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
                            AS DATE
                        ) AS prd_end_dt
                    FROM bronze.prd_info
                """),

                ("CRM Sales Details", """
                    INSERT INTO silver.crm_sales_details (
                        sls_ord_num,
                        sls_prd_key,
                        sls_cust_id,
                        sls_order_dt,
                        sls_ship_dt,
                        sls_due_dt,
                        sls_sales,
                        sls_quantity,
                        sls_price
                    )
                    SELECT 
                        sls_ord_num,
                        sls_prd_key,
                        sls_cust_id,
                        CASE 
                            WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS VARCHAR)) != 8 THEN NULL
                            ELSE CAST(
                                substr(CAST(sls_order_dt AS VARCHAR),1,4) || '-' ||
                                substr(CAST(sls_order_dt AS VARCHAR),5,2) || '-' ||
                                substr(CAST(sls_order_dt AS VARCHAR),7,2)
                            AS DATE)
                        END AS sls_order_dt,
                        CASE 
                            WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS VARCHAR)) != 8 THEN NULL
                            ELSE CAST(
                                substr(CAST(sls_ship_dt AS VARCHAR),1,4) || '-' ||
                                substr(CAST(sls_ship_dt AS VARCHAR),5,2) || '-' ||
                                substr(CAST(sls_ship_dt AS VARCHAR),7,2)
                            AS DATE)
                        END AS sls_ship_dt,
                        CASE 
                            WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS VARCHAR)) != 8 THEN NULL
                            ELSE CAST(
                                substr(CAST(sls_due_dt AS VARCHAR),1,4) || '-' ||
                                substr(CAST(sls_due_dt AS VARCHAR),5,2) || '-' ||
                                substr(CAST(sls_due_dt AS VARCHAR),7,2)
                            AS DATE)
                        END AS sls_due_dt,
                        CASE 
                            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
                                THEN sls_quantity * ABS(sls_price)
                            ELSE sls_sales
                        END AS sls_sales,
                        sls_quantity,
                        CASE 
                            WHEN sls_price IS NULL OR sls_price <= 0 
                                THEN sls_sales / NULLIF(sls_quantity, 0)
                            ELSE sls_price
                        END AS sls_price
                    FROM bronze.sales_details_info
                """),

                ("ERP Customer", """
                    INSERT INTO silver.erp_cust (
                        cid,
                        bdate,
                        gen
                    )
                    SELECT
                        CASE
                            WHEN cid LIKE 'NAS%' THEN SUBSTR(cid, 4)
                            ELSE cid
                        END AS cid,
                        CASE
                            WHEN bdate > CURRENT_DATE THEN NULL
                            ELSE bdate
                        END AS bdate,
                        CASE
                            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                            ELSE 'n/a'
                        END AS gen
                    FROM bronze.cust_erp
                """),

                ("ERP Location", """
                    INSERT INTO silver.erp_loc (
                        cid,
                        cntry
                    )
                    SELECT
                        REPLACE(cid, '-', '') AS cid,
                        CASE
                            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                            ELSE TRIM(cntry)
                        END AS cntry
                    FROM bronze.loc_erp
                """),

                ("ERP Product Category", """
                    INSERT INTO silver.erp_px_cat (
                        id,
                        cat,
                        subcat,
                        maintenance
                    )
                    SELECT
                        id,
                        cat,
                        subcat,
                        maintenance
                    FROM bronze.cat_erp
                """)
            ]

            print("Starting data transformations...")
            for i, (name, sql) in enumerate(transformations, 1):
                try:
                    print(f"Processing {name}...")
                    result = con.execute(sql)
                    rows_affected = result.fetchall() if hasattr(result, 'fetchall') else 'Unknown'
                    print(f"✓ {name} completed ({i}/{len(transformations)})")
                except Exception as e:
                    print(f"✗ Error processing {name}: {e}")
                    raise

            print("All transformations completed successfully")

            # Verify data was inserted
            print("\nVerifying data insertion:")
            verification_queries = [
                ("CRM Customer Info", "SELECT COUNT(*) FROM silver.crm_cust_info"),
                ("CRM Product Info", "SELECT COUNT(*) FROM silver.crm_prd_info"),
                ("CRM Sales Details", "SELECT COUNT(*) FROM silver.crm_sales_details"),
                ("ERP Customer", "SELECT COUNT(*) FROM silver.erp_cust"),
                ("ERP Location", "SELECT COUNT(*) FROM silver.erp_loc"),
                ("ERP Product Category", "SELECT COUNT(*) FROM silver.erp_px_cat")
            ]

            for table_name, query in verification_queries:
                try:
                    result = con.execute(query).fetchone()
                    count = result[0] if result else 0
                    print(f"  {table_name}: {count} rows")
                except Exception as e:
                    print(f"  {table_name}: Error counting rows - {e}")

    except Exception as e:
        print(f"Error during data processing: {e}")
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

    print("Data preprocessing completed successfully!")
