CREATE SCHEMA IF NOT EXISTS business;

CREATE TABLE IF NOT EXISTS business.cust_info (
    cst_id INTEGER,
    cst_key VARCHAR(100),
    cst_firstname VARCHAR(100),
    cst_lastname VARCHAR(100),
    cst_marital_status CHAR(1),
    cst_gndr CHAR(1),
    cst_create_date DATE
);

COPY business.cust_info(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date) 
FROM '/input_data/source_crm/cust_info.csv' DELIMITER ','  CSV HEADER;


CREATE TABLE IF NOT EXISTS business.prd_info (
    prd_id INTEGER,
    prd_key VARCHAR(100),
    prd_nm VARCHAR(100),
    prd_cost NUMERIC(10, 2),
    prd_line CHAR(1),
    prd_start_dt DATE,
    prd_end_dt DATE
);

COPY business.prd_info(prd_id, prd_key ,prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt) 
FROM '/input_data/source_crm/prd_info.csv' DELIMITER ','  CSV HEADER;


CREATE TABLE IF NOT EXISTS business.sales_details (
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

COPY business.sales_details(sls_ord_num, sls_prd_key ,sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price) 
FROM '/input_data/source_crm/sales_details.csv' DELIMITER ','  CSV HEADER;


CREATE TABLE IF NOT EXISTS business.cust_erp (
    CID TEXT,
    BDATE DATE,
    GEN TEXT
);

COPY business.cust_erp(CID, BDATE, GEN) 
FROM '/input_data/source_erp/CUST_AZ12.csv' DELIMITER ','  CSV HEADER;

CREATE TABLE IF NOT EXISTS business.loc_erp (
    CID TEXT,
    CNTRY TEXT
);

COPY business.loc_erp(CID, CNTRY) 
FROM '/input_data/source_erp/LOC_A101.csv' DELIMITER ','  CSV HEADER;

CREATE TABLE IF NOT EXISTS business.px_cat_erp (
    ID TEXT,
    CAT TEXT,
    SUBCAT TEXT,
    MAINTENANCE TEXT
);

COPY business.px_cat_erp(ID, CAT ,SUBCAT ,MAINTENANCE) 
FROM '/input_data/source_erp/PX_CAT_G1V2.csv' DELIMITER ','  CSV HEADER;