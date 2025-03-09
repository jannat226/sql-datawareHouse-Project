-- Creating the load_silver stored procedure
CREATE OR REPLACE PROCEDURE load_silver()
LANGUAGE plpgsql
AS $$
BEGIN
    -- RAISE NOTICE for the stored procedure execution
    RAISE NOTICE 'Executing the load_silver procedure...';

    -- Creating tables and inserting data

    -- crm_cust_info table
    DROP TABLE IF EXISTS silver.crm_cust_info;
    RAISE NOTICE 'Dropping and creating table: silver.crm_cust_info';

    CREATE TABLE silver.crm_cust_info (
        cst_id             INT,
        cst_key            VARCHAR(50),
        cst_firstname      VARCHAR(50),
        cst_lastname       VARCHAR(50),
        cst_marital_status VARCHAR(50),
        cst_gndr           VARCHAR(50),
        cst_create_date    DATE,
        dwh_create_date    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    RAISE NOTICE 'Table created: silver.crm_cust_info';

    -- crm_prd_info table
    DROP TABLE IF EXISTS silver.crm_prd_info;
    RAISE NOTICE 'Dropping and creating table: silver.crm_prd_info';

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
    );

    RAISE NOTICE 'Table created: silver.crm_prd_info';

    -- crm_sales_details table
    DROP TABLE IF EXISTS silver.crm_sales_details;
    RAISE NOTICE 'Dropping and creating table: silver.crm_sales_details';

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
    );

    RAISE NOTICE 'Table created: silver.crm_sales_details';

    -- erp_loc_a101 table
    DROP TABLE IF EXISTS silver.erp_loc_a101;
    RAISE NOTICE 'Dropping and creating table: silver.erp_loc_a101';

    CREATE TABLE silver.erp_loc_a101 (
        cid             VARCHAR(50),
        cntry           VARCHAR(50),
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    RAISE NOTICE 'Table created: silver.erp_loc_a101';

    -- erp_cust_az12 table
    DROP TABLE IF EXISTS silver.erp_cust_az12;
    RAISE NOTICE 'Dropping and creating table: silver.erp_cust_az12';

    CREATE TABLE silver.erp_cust_az12 (
        cid             VARCHAR(50),
        bdate           DATE,
        gen             VARCHAR(50),
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    RAISE NOTICE 'Table created: silver.erp_cust_az12';

    -- erp_px_cat_g1v2 table
    DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
    RAISE NOTICE 'Dropping and creating table: silver.erp_px_cat_g1v2';

    CREATE TABLE silver.erp_px_cat_g1v2 (
        id              VARCHAR(50),
        cat             VARCHAR(50),
        subcat          VARCHAR(50),
        maintenance     VARCHAR(50),
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    RAISE NOTICE 'Table created: silver.erp_px_cat_g1v2';

    -- Inserting data from bronze.crm_cust_info into silver.crm_cust_info
    INSERT INTO silver.crm_cust_info (
        cst_id, 
        cst_key, 
        cst_firstname, 
        cst_lastname, 
        cst_gndr, 
        cst_create_date
    )
    SELECT 
        cst_id, 
        cst_key, 
        cst_firstname, 
        cst_lastname, 
        cst_gndr, 
        cst_create_date
    FROM bronze.crm_cust_info
    LIMIT 1000;

    -- RAISE NOTICE after the insertion is done
    RAISE NOTICE 'Data inserted into silver.crm_cust_info from bronze.crm_cust_info';
    
END;
$$;

