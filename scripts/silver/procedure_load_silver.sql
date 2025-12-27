/*
========================================================
Stored Procedure: Load Silver Layer ( Bronze -> Silver)

========================================================
Script Purpose:
  This stored Procedure performs the ETL (Extract, Transform, Load) process to populate the 'silver' schema tables from the
  'bronze' schema.
Actions Performed:
  - Truncates Silver Tables.
  - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
  None.
  This stored procedure doesn't accept any parameters or return any values.

Usage example:
  EXEC silver.load_silver;
========================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE ();
		PRINT '==================================================';
		PRINT 'Loading Silver Layer'
		PRINT '==================================================';

		PRINT '--------------------------------------------------';
		PRINT 'loading CRM Tables';
		PRINT '--------------------------------------------------';

		-- Loading silver.crm_cust_info
		SET @start_time = GETDATE ()
		PRINT '>> TRUNCATING TABLE: silver.crm_cust_info';
		TRUNCATE table silver.crm_cust_info;
		PRINT '>> Inserting Data into: silver.crm_cust_info';
		INSERT into silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

		select
			cst_id,
			cst_key,
			TRIM (cst_firstname) as cst_firstname,
			TRIM (cst_lastname) as cst_lastname,
			case when upper(TRIM(cst_marital_status)) = 'S' then 'Single'
				 when upper(TRIM(cst_marital_status)) = 'M' then 'Married'
				 Else 'n/a'
			END cst_marital_status,
			case when upper(TRIM(cst_gndr)) = 'F' then 'Female'
				 when upper(TRIM(cst_gndr)) = 'M' then 'Male'
				 Else 'n/a'
			END cst_gndr, 
			cst_create_date
		From(
			select
			*,
			ROW_NUMBER () over (partition by cst_id order by cst_create_date DESC) AS flag_last
			from bronze.crm_cust_info
			where cst_id is not null
		)t where flag_last = 1;
		SET @end_time = GETDATE ();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as VARCHAR) + ' seconds'
		PRINT '>> ----------------';

		-- Loading silver.crm_prd_info
		SET @start_time = GETDATE ()
		PRINT '>> TRUNCATING TABLE: silver.crm_prd_info';
		TRUNCATE table silver.crm_prd_info;
		PRINT '>> Inserting Data into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		select
		prd_id,
		REPLACE (SUBSTRING(prd_key, 1, 5), '-', '_') As cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,
		prd_nm,
		ISNULL (prd_cost, 0) as prd_cost,
		CASE UPPER(TRIM(prd_line)) 
			 WHEN 'M' then 'Mountain'
			 WHEN 'R' then 'Road'
			 WHEN 's' then 'Other sales'
			 WHEN 'T' then 'Touring'
			 else 'n/a'
		END as prd_line,
		CAST (prd_start_dt AS date) as prd_start_dt,
		CAST (LEAD (prd_start_dt) OVER (PARTITION by prd_key order by prd_start_dt)-1 AS date) as prd_end_dt
		from bronze.crm_prd_info;
		SET @end_time = GETDATE ();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as VARCHAR) + ' seconds'
		PRINT '>> ----------------';

		-- Loading silver.crm_sales_details
		SET @start_time = GETDATE ()
		PRINT '>> TRUNCATING TABLE: silver.crm_sales_details';
		TRUNCATE table silver.crm_sales_details;
		PRINT '>> Inserting Data into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
				sls_order_num,
				sls_prd_key,		
				sls_cust_id,	
				sls_order_dt,	
				sls_ship_date,	
				sls_due_dt,
				sls_sales,	
				sls_quantity,	
				sls_price
		)
		select
		sls_order_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN (sls_order_dt) != 8 then null
			 ELSE CAST(CAST (sls_order_dt as varchar) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN (sls_ship_dt) != 8 then null
			 ELSE CAST(CAST(sls_ship_dt as VARCHAR) AS date)
		END AS sls_ship_dt, 
		CASE WHEN sls_due_dt = 0 OR LEN (sls_due_dt) ! = 8 then null
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales IS null OR sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		ELSE  sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price is null or sls_price <= 0
			THEN sls_sales/ nullif(sls_quantity, 0)
			ELSE sls_price
		END AS sls_price
		from bronze.crm_sales_details;
		SET @end_time = GETDATE ();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as VARCHAR) + ' seconds'
		PRINT '>> ----------------';

		PRINT '--------------------------------------------------';
		PRINT 'loading ERP Tables';
		PRINT '--------------------------------------------------';

		--Loading silver.erp_cust_az12
		SET @start_time = GETDATE ()
		PRINT '>> TRUNCATING TABLE: silver.erp_cust_az12';
		TRUNCATE table silver.erp_cust_az12;
		PRINT '>> Inserting Data into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
		select
		CASE WHEN cid like 'NAS%' THEN SUBSTRING (cid, 4, LEN(cid))
			 ELSE cid
		END cid,
		CASE WHEN bdate > GETDATE () then NULL
			ELSE bdate
		END bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') then 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') then 'Male'
			ELSE 'n/a'
		END AS gen
		from bronze.erp_cust_az12;
		SET @end_time = GETDATE ();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as VARCHAR) + ' seconds'
		PRINT '>> ----------------';

		--Loading silver.erp_loc_a101
		SET @start_time = GETDATE ()
		PRINT '>> TRUNCATING TABLE: silver.erp_loc_a101';
		TRUNCATE table silver.erp_loc_a101;
		PRINT '>> Inserting Data into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(
		cid,
		cntry)
		select
		REPLACE (cid, '-', '') cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' or cntry is null THEN 'n/a'
			 ELSE TRIM(cntry)
		END as cntry
		from bronze.erp_loc_a101;
		SET @end_time = GETDATE ();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as VARCHAR) + ' seconds'
		PRINT '>> ----------------';

		-- Loading silver.erp_px_cat_g1v2
		SET @start_time = GETDATE ()
		PRINT '>> TRUNCATING TABLE: silver.erp_px_cat_g1v2';
		TRUNCATE table silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance)
		select
		Id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE ();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>--------------';

		SET @batch_end_time = GETDATE ();
		PRINT '========================================================='
		PRINT 'Loading Silver Table is Completed';
		PRINT '========================================================='

	END TRY
	BEGIN CATCH
		PRINT '========================================================='
		PRINT 'ERROR OCCURED DURING DURING BRONZE LAYER'
		PRINT 'Error Message' + Error_Message ();
		PRINT 'Error Message' + CAST (ERROR_NUMBER () AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE () AS NVARCHAR);
		PRINT '=========================================================='
	END CATCH
END
