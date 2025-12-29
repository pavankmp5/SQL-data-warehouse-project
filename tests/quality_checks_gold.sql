/*
=======================================================================
Quality checks
=======================================================================
Script purpose:
  This script performs quality checks to validate the integrity, consistency, and accuracy of the Gold layer also 
  these checks ensure:
  - Uniqueness of surrogate keys in dimension tables.
  - Referential integrity between fact and dimension table.
  - Validation of relationships in the data model for analytical purposes.

Usage notes:
  - Run these checks after loading data into the silver layers.
  - Investigate and resolve any discrepancies found during the checks.
=========================================================================
*/

-- =====================================================================
-- Checking 'gold.dim_customers'
-- =====================================================================
-- Check for Uniqueness of Customer key in gold.dim_customers
-- Expectations: No results
SELECT
  customer_key,
  COUNT (*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT (*) > 1;

-- =====================================================================
-- Checking 'gold.product_key'
-- =====================================================================
-- Check for Uniqueness of product key in gold.diM_products
-- Expectations: No results
SELECT
  product_key,
  COUNT (*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT (*) > 1;

-- =====================================================================
-- Checking 'gold.fact_sales'
-- =====================================================================
-- Check the data model connectivity between fact and dimensions
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL



