/*
==================================================================================
Quality Checks
==================================================================================
Script Purpose:
	Tis script performs various quality checks for data consistency , accuracy , 
	and standardization  across the 'silver' schema . It includes checks for :
	- Null or duplicate primary key .
	- Unwanted spaces in string fields . 
	- Data standardization and consistency . 
	- Invaild data ranges and orders .
	- Data consistency between related fields .


Usage Notes :
	- Run these checks after data loading Silver Layer .
	- Invistigate and resolve any discrepancies found during the checks .
	==================================================================================
*/



--######################################################################################
--						bronze.crm_cust_info

-- CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY 
-- EXPECTATIONS : NO RESULTS

SELECT cst_id , COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


-- CHECK FOR UNWANTED SPACES
-- EXPECTATIONS : NO RESULTS
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT DISTINCT cst_material_status
FROM silver.crm_cust_info

SELECT * 
FROM silver.crm_cust_info
WHERE cst_id  = 29483


SELECT * 
FROM silver.crm_cust_info
WHERE cst_material_status IS NULL

--######################################################################################
--						bronze.crm_prd_info

--	CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY 
--	EXPECTATION : NO RESULTS
SELECT 
prd_id , 
COUNT(*) AS COUNT_OF_RECORDS
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL


-- CHECK FOR ANY UNWANTED SPACES 
-- EXPECTATION : NO RESULTS

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- CHECK FOR NULLS OR NEGATINE NUMBERS 
-- EXPECTATION : NO RESULTS 

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

SELECT prd_line
FROM silver.crm_prd_info
WHERE prd_line != TRIM(prd_line)

-- CHECK FOR INVALID DATE ORDERS 
SELECT * 
FROM silver.crm_prd_info
WHERE prd_start_dt < prd_end_dt

SELECT *
FROM silver.crm_prd_info
--######################################################################################
--						bronze.crm_sales_details


-- CHECK FOR INVALID DATES

SELECT 
NULLIF(sls_ship_dt , 0) sls_ship_dt 
FROM silver.crm_sales_details
WHERE sls_ship_dt <=0 OR sls_ship_dt !=8 
OR sls_ship_dt > 20500101 
OR sls_ship_dt < 19000101

SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

SELECT DISTINCT
sls_sales AS OLD_SALES , sls_quantity,sls_price AS OLD_PRICE,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
	 THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales , 
CASE WHEN sls_price IS NULL OR sls_price <=0 
	 THEN sls_sales / NULLIF(sls_quantity , 0)
	 ELSE sls_price
END AS sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL 
OR sls_quantity IS NULL 
OR sls_price IS NULL
OR sls_sales <=0 
OR sls_quantity <=0
OR sls_price <=0
ORDER BY sls_sales , sls_quantity , sls_price

SELECT * FROM silver.crm_sales_details

--######################################################################################
--						bronze.erp_cust_az12



-- CHECK FOR CID 
-- EXPECTITION : FAMILIR WITH CST_KEY COLUMN IN crm_cust_info TABLE 
SELECT cid FROM silver.erp_cust_az12
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info)

--CHECK FOR bdate 
--EXPECTITION : NO RESULT LESS THAN '1924-01-01' OR GREATER THEN NOW .

SELECT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()


-- CHECK GEN 
-- EXPECTATION : STANDARD DATA (Female , Male , n/a)

SELECT DISTINCT gen
FROM silver.erp_cust_az12


SELECT * FROM silver.erp_cust_az12

--######################################################################################
--						bronze.erp_loc_a101

-- CHECK CID
-- EXPECTITION : NO RESULT

SELECT cid
FROM silver.erp_loc_a101
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info)


--DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT cntry 
FROM silver.erp_loc_a101
ORDER BY cntry

SELECT * FROM silver.erp_loc_a101

--######################################################################################
--						bronze.erp_px_cat_g1v2

-- CHECK FOR UNWANTED SPACES 
-- EXPECTATION : NO RESULT
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- DATA SDANDARDIZATION & CONSISTENCY
SELECT DISTINCT maintenance
FROM silver.erp_px_cat_g1v2

SELECT * FROM silver.erp_px_cat_g1v2
