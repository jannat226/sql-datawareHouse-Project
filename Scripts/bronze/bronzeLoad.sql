DROP PROCEDURE IF EXISTS bronze.load_bronze;

CREATE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE 
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
    table_start TIMESTAMP;
    table_end TIMESTAMP;
    table_duration INTERVAL;
BEGIN
    -- Log Start Time
    start_time := clock_timestamp();

    -- Start Logging
    RAISE NOTICE '--------------------';
    RAISE NOTICE 'Starting Bronze Layer Loading';
    RAISE NOTICE '--------------------';

    -- Table: crm_cust_info
    table_start := clock_timestamp();
    RAISE NOTICE 'Start Creating Table: crm_cust_info at %', table_start;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'crm_cust_info') THEN
        CREATE TABLE bronze.crm_cust_info(
            cst_id INT,
            cst_key VARCHAR(50),
            cst_firstname VARCHAR(50),
            cst_lastname VARCHAR(50),
            cst_material_status VARCHAR(50),
            cst_gndr VARCHAR(50),
            cst_create_date DATE
        );
    END IF;

    table_end := clock_timestamp();
    table_duration := table_end - table_start;
    RAISE NOTICE 'Completed Creating Table: crm_cust_info at %. Duration: % seconds', table_end, EXTRACT(SECOND FROM table_duration);

    -- Table: crm_sales_details
    table_start := clock_timestamp();
    RAISE NOTICE 'Start Creating Table: crm_sales_details at %', table_start;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'crm_sales_details') THEN
        CREATE TABLE bronze.crm_sales_details(
            sls_ord_num VARCHAR(50),
            sls_prd_key VARCHAR(50),
            sls_cust_id INT,
            sls_order_dt INT,
            sls_ship_dt INT,
            sls_due_dt DATE,
            sls_sales VARCHAR(50),
            sls_quantity INT,
            sls_price INT
        );
    END IF;

    table_end := clock_timestamp();
    table_duration := table_end - table_start;
    RAISE NOTICE 'Completed Creating Table: crm_sales_details at %. Duration: % seconds', table_end, EXTRACT(SECOND FROM table_duration);

    -- Table: crm_prd_info
    table_start := clock_timestamp();
    RAISE NOTICE 'Start Creating Table: crm_prd_info at %', table_start;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'crm_prd_info') THEN
        CREATE TABLE bronze.crm_prd_info(
            prd_id INT,    
            prd_key VARCHAR(50),
            prd_nm VARCHAR(50),
            prd_cost INT,
            prd_line VARCHAR(50),
            prd_start_dt TIMESTAMP,
            prd_end_dt TIMESTAMP
        );
    END IF;

    table_end := clock_timestamp();
    table_duration := table_end - table_start;
    RAISE NOTICE 'Completed Creating Table: crm_prd_info at %. Duration: % seconds', table_end, EXTRACT(SECOND FROM table_duration);

    -- Table: erp_cust_az12
    table_start := clock_timestamp();
    RAISE NOTICE 'Start Creating Table: erp_cust_az12 at %', table_start;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'erp_cust_az12') THEN
        CREATE TABLE bronze.erp_cust_az12(
            CID INT,
            BDATE DATE,
            GEN VARCHAR(50)
        );
    END IF;

    table_end := clock_timestamp();
    table_duration := table_end - table_start;
    RAISE NOTICE 'Completed Creating Table: erp_cust_az12 at %. Duration: % seconds', table_end, EXTRACT(SECOND FROM table_duration);

    -- Table: erp_loc_a101
    table_start := clock_timestamp();
    RAISE NOTICE 'Start Creating Table: erp_loc_a101 at %', table_start;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'erp_loc_a101') THEN
        CREATE TABLE bronze.erp_loc_a101(
            CID INT,
            CNTRY VARCHAR(50)
        );
    END IF;

    table_end := clock_timestamp();
    table_duration := table_end - table_start;
    RAISE NOTICE 'Completed Creating Table: erp_loc_a101 at %. Duration: % seconds', table_end, EXTRACT(SECOND FROM table_duration);

    -- Table: erp_px_cat_g1v2
    table_start := clock_timestamp();
    RAISE NOTICE 'Start Creating Table: erp_px_cat_g1v2 at %', table_start;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'erp_px_cat_g1v2') THEN
        CREATE TABLE bronze.erp_px_cat_g1v2(
            ID VARCHAR(50),
            CAT VARCHAR(50),
            SUBCAT VARCHAR(50),
            MAINTENANCE VARCHAR(50)
        );
    END IF;

    table_end := clock_timestamp();
    table_duration := table_end - table_start;
    RAISE NOTICE 'Completed Creating Table: erp_px_cat_g1v2 at %. Duration: % seconds', table_end, EXTRACT(SECOND FROM table_duration);

    -- Log End Time
    end_time := clock_timestamp();
    duration := end_time - start_time;

    -- Log Execution Duration
    RAISE NOTICE '--------------------';
    RAISE NOTICE 'Bronze Layer Loading Completed';
    RAISE NOTICE 'Total Execution Time: % seconds (% milliseconds)', EXTRACT(SECOND FROM duration), EXTRACT(MILLISECOND FROM duration);
    RAISE NOTICE '--------------------';

EXCEPTION 
    WHEN OTHERS THEN
        RAISE NOTICE 'An error occurred during loading bronze: %', SQLERRM;
END;
$$;
