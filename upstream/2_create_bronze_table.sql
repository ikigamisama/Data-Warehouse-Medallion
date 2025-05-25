\connect business

CREATE TABLE IF NOT EXISTS bronze.cust_info (
    cst_id INTEGER,
    cst_key VARCHAR(100),
    cst_firstname VARCHAR(100),
    cst_lastname VARCHAR(100),
    cst_marital_status CHAR(1),
    cst_gndr CHAR(1),
    cst_create_date DATE
);

CREATE TABLE IF NOT EXISTS bronze.prd_info (
    prd_id INTEGER,
    prd_key VARCHAR(100),
    prd_nm VARCHAR(100),
    prd_cost NUMERIC(10, 2),
    prd_line CHAR(1),
    prd_start_dt DATE,
    prd_end_dt DATE
);

CREATE TABLE IF NOT EXISTS bronze.sales_details_info (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt VARCHAR(50),
    sls_ship_dt VARCHAR(50),
    sls_due_dt VARCHAR(50),
    sls_sales NUMERIC(10, 2),
    sls_quantity INT,
    sls_price NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS bronze.cust_erp (
    CID TEXT,
    BDATE DATE,
    GEN TEXT
);

CREATE TABLE IF NOT EXISTS bronze.loc_erp (
    CID TEXT,
    CNTRY TEXT
);

CREATE TABLE IF NOT EXISTS bronze.cat_erp (
    ID TEXT,
    CAT TEXT,
    SUBCAT TEXT,
    MAINTENANCE TEXT
);