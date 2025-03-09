-- DDL COMMANDS by understanding the meta data || defining the structure of the dataset
/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/



DO $$
BEGIN
    -- Check if the table exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'crm_cust_info') THEN
        -- Create the table if it doesn't exist
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
END $$;

DO $$
BEGIN
    -- Check if the table exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'crm_sales_details') THEN
        -- Create the table if it doesn't exist
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
END $$;


DO $$
BEGIN
    -- Check if the table exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'crm_prd_info') THEN
        -- Create the table if it doesn't exist
		CREATE TABLE bronze.crm_prd_info (
			prd_id INT,    
			prd_key VARCHAR(50),
			prd_nm VARCHAR(50),
			prd_cost INT,
			prd_line VARCHAR(50),
			prd_start_dt TIMESTAMP,  -- Use TIMESTAMP instead of DATETIME in PostgreSQL
			prd_end_dt TIMESTAMP     -- Use TIMESTAMP instead of DATETIME in PostgreSQL
		);
    END IF;
END $$;

DO $$
BEGIN
    -- Check if the table exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'erp_cust_az12') THEN
        -- Create the table if it doesn't exist
		CREATE TABLE bronze.erp_cust_az12(
		CID	INT,
		BDATE DATE,
		GEN VARCHAR(50));
  END IF;
END $$;

DO $$
BEGIN
    -- Check if the table exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'erp_loc_a101') THEN
        -- Create the table if it doesn't exist
		CREATE TABLE bronze.erp_loc_a101(
		CID	INT,
		CNTRY VARCHAR(50)
		);
 END IF;
END $$;


DO $$
BEGIN
    -- Check if the table exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'erp_px_cat_g1v2') THEN
        -- Create the table if it doesn't exist
	CREATE TABLE bronze.erp_px_cat_g1v2(
	ID	VARCHAR(50),
	CAT VARCHAR(50),
	SUBCAT VARCHAR(50),
	MAINTENANCE VARCHAR(50)
	);
END IF;
END $$



