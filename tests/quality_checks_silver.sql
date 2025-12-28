/*
==============================================================
Quality checks
==============================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracy, and standardisations
across the silver  schema. It includes checks for:
  - Null or duplicate primary keys.
  - Unwanted spaces in string fields.
  - Data standardisation and consistency.
  - Invalid date ranges and orders.
  - Data consistency between related fields.

Usage notes:
  - Run these checks after data loading in the 'silver' layer.
  - Investigate and resolve any discrepancies found during the checks.
=================================================================
*/

-- ==============================================================
-- 'checking 'silver.crm_cust_info'
=================================================================
-- Check for nulls or duplicates in Primary key
-- Expectation: No results

SELECT
  cst_id,
  count (*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT (*) > 1 OR cst_id is NULL;

-- Check for unwanted spaces
-- Expectation:" No results
SELECT
  cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Data standardisations & consistency
SELECT DISTINCT
  cst_marital_status
FROM silver.crm_cust_info;

-- ===============================================
-- Checking 'silver.crm_prd_info'
-- ===============================================
-- Checks for Nulls or Duplicates in Primary key
-- Expectation: No results

SELECT
  prd_id,
  count (*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT (*) > 1 or prd_id IS NULL;

-- Check for Unwanted Spaces
-- Expectations: No Results
SELECT
  prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM (prd_nm);

-- Check for Nulls or Negative Values in cost
-- Expectations: No Results
SELECT
  prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL;

-- Data standardisation & Consistency
-- Expectations: No Results
SELECT DISTINCT
  prd_line
FROM silver.crm_prd_info;

-- Check for Invalid order Dates (startdate > enddate)
-- Expectations: No Results
SELECT 
  *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- =========================================================
-- Checking 'silver.crm_sales_details'
-- =========================================================
-- Check for Invalid dates
-- Expectations: No invalid dates should occur
SELECT
  NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
  OR LEN(sls_due_dt) != 8
  OR sls_due_dt > 20500101
  OR sls_due_dt < 19000101;

-- Check for Invalid order Dates (order date > shipping/ due dates)
-- Expectations: No results
SELECT 
  *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_date
  OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Sales = quantity * price
--  Expectation: No results
SELECT DISTINCT
  sls_sales,
  sls_quantity,
  sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * price
  OR sls_sales IS NULL
  OR sls_quantity IS NULL
  OR sls_price IS NULL
  OR sls_sales <= 0
  OR sls_quantity <= 0,
  OR sls_price <= 0
ORDER BY   sls_sales, sls_quantity, sls_price;

-- ==================================================
-- Checking 'silver.erp_cust_az12'
-- ==================================================
-- Identify Out-of-range Dates
-- Expectation: Birthdates between 1924-01-01 and Current date
SELECT DISTINCT 
  bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01'
  or  bdate > GETDATE ();

-- Data Standardisation & Consistency
SELECT DISTINCT
  gen
FROM silver.erp_cust_az12;

-- ===================================================
Checking 'silver.erp_loc_a101'
-- ===================================================
-- Data Standardisation & Consistency
SELECT
  cntry
FROM silver.erp_cust_a101
ORDER BY cntry;

-- ===================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ===================================================
-- Check for Unwanted Spaces
-- Expectations: No Results
SELECT
  *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
  OR  subcat != TRIM(subcat)
  OR maintenance != TRIM(maintenance);

-- Data Standardisation & Consitency
SELECT DISTINCT
  maintenance
FROM silver.erp_px_cat_g1v2;



