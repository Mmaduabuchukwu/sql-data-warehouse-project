/*
===============================================================================
Data transformation on silver level
===============================================================================
At the silver level, various data transformation processes were performed, including:
    * Identifying duplicates in the dataset
    * Performing the deduplication process
    * Removing unwanted spaces from the dataset
    * Ensuring data standardization and consistency

Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/
--Data Transformation script from the BRONZE LAYER to the SILVER LAYER
/* ======== CRM CUSTOMER DETAILS*/
/* 
Check for Null or Duplicate in the primary key from the bronze
Expectation: No Result
*/
--Finding duplicate
Select 
cst_id,
COUNT(*)
From bronze.crm_cust_info
Group by cst_id
Having Count(*) > 1 or cst_id is null

--Dedup process
Select 
* 
From ( 
Select 
*,
Row_Number() over(partition by cst_id order by cst_create_date Desc) as flag_last
From bronze.crm_cust_info
where cst_id is not null
)t where flag_last = 1

--Check for unwanted spaces
--Expectation: No result
Select cst_firstname
From bronze.crm_cust_info
where cst_firstname != Trim(cst_firstname)

Select cst_lastname
From bronze.crm_cust_info
where cst_lastname != Trim(cst_lastname)

Select cst_gndr
From bronze.crm_cust_info
where cst_gndr != Trim(cst_gndr)

Select cst_key
From bronze.crm_cust_info
where cst_key != Trim(cst_key)

--Data Standardization & consistency -- use DISTINCT FUNCTION 
Select DISTINCT cst_gndr
From bronze.crm_cust_info

Select DISTINCT cst_material_status
From bronze.crm_cust_info


---Transformation to clean up the column from bronze layer to silver layer
Select
cst_id,
cst_key,
Trim(cst_firstname) AS cst_firstname,
Trim(cst_lastname) AS cst_lastname,
CASE
    WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single' --This is where the alphabet is ina  smaller letter or there is space in front of the aphebet
	When UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
	ELSE 'n/a'
END cst_material_status,
CASE
    WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' --This is where the alphabet is in smaller letters or there is space in front of the alphabet
	When UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
	Select
	*,
	Row_Number() over(partition by cst_id order by cst_create_date Desc) as flag_last
	From bronze.crm_cust_info
	where cst_id is not null
)t where flag_last = 1

---INSERT INTO CRM CUSTOMER SILVER LAYER 
PRINT '>> Truncating Table: silver.crm_cust_info'
TRUNCATE TABLE silver.crm_cust_info
PRINT '>> Inserting Data Into: silver.crm_cust_info'
INSERT INTO silver.crm_cust_info(
	   cst_id, 
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gndr,
		cst_create_date)

Select
cst_id,
cst_key,
Trim(cst_firstname) AS cst_firstname,
Trim(cst_lastname) AS cst_lastname,
CASE
    WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single' --This is where the alphabet is in a smaller letter or there is space in front of the alphabet
	When UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
	ELSE 'n/a'
END cst_material_status, --Normalize marital status values to a readable format
CASE
    WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' --This is where the alphabet is in smaller letters or there is space in front of the alphabet
	When UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'n/a'
END cst_gndr, --Normalize gender values to a readable format
cst_create_date
FROM (
	Select
	*,
	Row_Number() over(partition by cst_id order by cst_create_date Desc) as flag_last
	From bronze.crm_cust_info
	where cst_id is not null
)t where flag_last = 1  --Select the most recent record per customer

/* ======== CRM PRODUCT DETAILS*/
/* 
Check for Null or Duplicate in the primary key from the bronze
Expectation: No Result
*/
--Finding duplicate
Select 
prd_id,
COUNT(*)
From bronze.crm_prd_info
Group by prd_id
Having Count(*) > 1 or prd_id is null

--Check for unwanted Spaces
--Expectation:No Results
Select prd_nm
From bronze.crm_prd_info
where prd_nm != Trim(prd_nm)

--Check for Nulls or Negative Numbers
--Expectation: No Results
Select prd_cost
From bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null

--Data Standardization & Consistency
Select DISTINCT prd_line
From bronze.crm_prd_info

Select *
From bronze.crm_prd_info
where prd_end_date < prd_start_dt

Select *
From bronze.crm_prd_info
where prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509')

Select *,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key Order By prd_start_dt)-1 as prd_end_dt_test
From bronze.crm_prd_info
where prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509')

--first process to find NOT MATCHING in erp category table using NOT IN
SELECT [prd_id]
      ,[prd_key]
	  ,REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') AS cat_id-- SUBSTRING() Extracts specific part of a string value
      ,[prd_nm]
      ,[prd_cost]
      ,[prd_line]
      ,[prd_start_dt]
      ,[prd_end_date]
  FROM [DataWarehouse].[bronze].[crm_prd_info]
  WHERE REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') NOT IN --Filters out unmatched data after applying transformation
(SELECT DISTINCT id from bronze.erp_px_cat_g1v2)

--second process to find a match in CRM sales details using IN
SELECT [prd_id]
      ,[prd_key]
	  ,REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') AS cat_id-- SUBSTRING() Extracts specific part of a string value
      ,SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
	  ,[prd_nm]
      ,[prd_cost]
      ,[prd_line]
      ,[prd_start_dt]
      ,[prd_end_date]
  FROM [DataWarehouse].[bronze].[crm_prd_info]
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN 
(Select sls_prd_key from bronze.crm_sales_details)
 
 --third process to find a match in CRM sales details using IN
SELECT [prd_id]
      ,[prd_key]
	  ,REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') AS cat_id-- SUBSTRING() Extracts specific part of a string value
      ,SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
	  ,[prd_nm]
      ,ISNULL([prd_cost],0) as prd_cost --Replaces NULL values with a specified replacement value
      ,CASE UPPER(TRIM(prd_line))  --Quick CASE WHEN ideal for simple value mapping
	       WHEN 'M' THEN 'Mountain'
		   WHEN 'R' THEN 'Road'
		   WHEN 'S' THEN 'Other Sales'
		   WHEN 'T' THEN 'Touring'
		   ELSE 'n/a'
	   END AS prd_line
      ,CAST([prd_start_dt] as Date) prd_start_dt -----converting the column as a date
      ,CAST(LEAD(prd_start_dt) oVER (PARTITION BY prd_key Order By prd_start_dt)-1 AS DATE) as prd_end_dt
  FROM [DataWarehouse].[bronze].[crm_prd_info]
  

--INSERT PRODUCT DATA INTO SILVER LAYER
PRINT '>> Truncating Table: silver.crm_prd_info'
TRUNCATE TABLE silver.crm_prd_info  -- the purpose of truncating a table is not to generate duplicate when running the table
PRINT '>> Inserting Data Into: silver.crm_prd_info'
INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_date)
SELECT [prd_id]
	  , REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') AS cat_id-- SUBSTRING() Extracts specific part of a string value / derive new column
      ,SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key --derive new column
	  ,[prd_nm]
      ,ISNULL([prd_cost],0) as prd_cost --Replaces NULL values with a specified replacement value
      ,CASE UPPER(TRIM(prd_line))  --Quick CASE WHEN ideal for simple value mapping
	       WHEN 'M' THEN 'Mountain'
		   WHEN 'R' THEN 'Road'
		   WHEN 'S' THEN 'Other Sales'
		   WHEN 'T' THEN 'Touring'
		   ELSE 'n/a'
	   END AS prd_line
      ,CAST([prd_start_dt] as Date) prd_start_dt -----converting the column as a date
      ,CAST(LEAD(prd_start_dt) oVER (PARTITION BY prd_key Order By prd_start_dt)-1 AS DATE) as prd_end_dt
  FROM [DataWarehouse].[bronze].[crm_prd_info]

 --======== CRM SALES DETAILS
 --DATA PROFILING
SELECT [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_Price]
  FROM [DataWarehouse].[bronze].[crm_sales_details]
  where sls_ord_num != TRIM(sls_ord_num)

--CONFIRMING THE PRD_KEY IN PRODUCT TABLE AND SALES TABLE
SELECT sls_prd_key 
FROM [DataWarehouse].[bronze].[crm_sales_details]
where sls_PRd_key NOT IN (SELECT prd_key from silver.crm_prd_info)

--CONFIRMING THE cust_id IN Customer TABLE AND SALES TABLE
SELECT sls_cust_id 
FROM [DataWarehouse].[bronze].[crm_sales_details]
where sls_cust_id NOT IN (SELECT cst_id  from silver.crm_cust_info)

--Check for invalid Dates
Select
NULLIF (sls_order_dt,0) sls_order_dt
From bronze.crm_sales_details
where sls_order_dt <= 0 or LEN (sls_order_dt) != 8 /*--Search the length of the date which must be 8 in length*/ or sls_order_dt > 20500101 

/* check for outlier by validating the boundaries of the data range when business started or ending */
Select
NULLIF (sls_order_dt,0) sls_order_dt
From bronze.crm_sales_details 
where sls_order_dt > 20500101 or sls_order_dt < 19000101 

--COMPLETE TRANSFORMATION FOR ORDER DATE
Select
NULLIF (sls_order_dt,0) sls_order_dt
From bronze.crm_sales_details 
where sls_order_dt <= 0 
OR LEN (sls_order_dt) != 8
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101 

--COMPLETE TRANSFORMATION FOR SHIP DATE
Select
NULLIF (sls_ship_dt,0) sls_ship_dt
From bronze.crm_sales_details 
where sls_ship_dt <= 0 
OR LEN (sls_ship_dt) != 8
OR sls_ship_dt > 20500101 
OR sls_ship_dt < 19000101 

--COMPLETE TRANSFORMATION FOR DUE DATE
Select
NULLIF (sls_ship_dt,0) sls_ship_dt
From bronze.crm_sales_details 
where sls_ship_dt <= 0 
OR LEN (sls_ship_dt) != 8
OR sls_ship_dt > 20500101 
OR sls_ship_dt < 19000101 

---CHECK FOR OREDR DATE AND SHIP DATE(OREDR DATE must always be earlier or smaller than the SHIPPING DATE OR DUE DATE)
--INVALID DATE ORDERS
SELECT
* 
FROM bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

--Check Data Consistency: Between Sales, Quantity, and Price
-->> Sales = Quantity * Price
-->> Values must not be Null, Zero or Negative

Select DISTINCT
sls_sales,
sls_quantity,
sls_Price
FROM bronze.crm_sales_details
where sls_sales != sls_quantity * sls_Price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_Price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_Price <= 0
ORDER BY 1

Select DISTINCT
sls_sales old_sls_sales,
sls_quantity,
sls_Price old_sls_price,
Case 
   WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_Price)--ABS:Returns absolute value of number
   THEN sls_quantity * ABS(sls_Price)
   Else sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <= 0
     THEN sls_sales / NULLIF(sls_quantity,0)
	 ELSE sls_price
End as sls_price

FROM bronze.crm_sales_details
where sls_sales != sls_quantity * sls_Price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_Price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_Price <= 0
ORDER BY sls_sales,sls_quantity, sls_Price

--TRANSFORMED SALES DETAILS DATASET TO BE INSERTED
PRINT '>> Truncating Table: silver.crm_sales_details'
TRUNCATE TABLE silver.crm_sales_details
PRINT '>> Inserting Data Into: silver.crm_sales_details'
INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_Price
)
SELECT [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,CASE
	      WHEN sls_order_dt = 0 OR LEN (sls_order_dt) != 8 THEN NULL
		  ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) ---To covert an Int to a DATE 1. Concert to VARCHAR and Conver VARCHE TO A DATE
		END AS sls_order_dt
      ,CASE
	      WHEN sls_ship_dt = 0 OR LEN (sls_ship_dt) != 8 THEN NULL
		  ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) ---To covert an Int to a DATE 1. Concert to VARCHAR and Conver VARCHE TO A DATE
		END AS sls_ship_dt
	  ,CASE
	      WHEN sls_due_dt = 0 OR LEN (sls_due_dt) != 8 THEN NULL
		  ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) ---To covert an Int to a DATE 1. Concert to VARCHAR and Conver VARCHE TO A DATE
		END AS sls_due_dt
      ,Case 
		   WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_Price)--ABS:Returns absolute value of number
		   THEN sls_quantity * ABS(sls_Price)
		   Else sls_sales
		END AS sls_sales
      ,[sls_quantity]
      ,CASE WHEN sls_price IS NULL OR sls_price <= 0
			 THEN sls_sales / NULLIF(sls_quantity,0)
			 ELSE sls_price
		End as sls_price
  FROM [DataWarehouse].[bronze].[crm_sales_details]

--======== ERP CUST
 Select 
 cid,
 bdate,
 gen
 From bronze.erp_cust_az12
 where cid like '%AW00011009%'  -- using the cust_key crm_cust to find out in the like is in erp_cust

 select * from silver.crm_cust_info
 ---Transformation of cid column
 Select 
 cid,
 CASE
     WHEN cid Like 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	 ELSE cid
END cid,
 bdate,
 gen
 From bronze.erp_cust_az12
 WHERE  CASE
     WHEN cid Like 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	 ELSE cid
END NOT IN(SELECT DISTINCT cst_key from silver.crm_cust_info)
 
--Identify Out-of-Range Date
 SELECT DISTINCT 
 bdate,
 CASE
     WHEN bdate > GETDATE() Then Null
	 else bdate
END bdate
 From bronze.erp_cust_az12
 WHERE bdate < '1924-01-01' OR bdate > GETDATE() -- check fof birthdays in the future

 --DATA STANDARDIZATION & CONSISTENCY
 Select DISTINCT gen,
 CASE 
     WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
 END gen
 From bronze.erp_cust_az12

---TRANSFORMED DATA TO BE INSERTED INTO ERP CUST
PRINT '>> Truncating Table: silver.erp_cust_az12'
TRUNCATE TABLE silver.erp_cust_az12
PRINT '>> Inserting Data Into: silver.erp_cust_az12'
INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
Select 
 CASE
     WHEN cid Like 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	 ELSE cid
END cid,
CASE
     WHEN bdate > GETDATE() Then Null
	 else bdate
END bdate,
 CASE 
     WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
 END gen
From bronze.erp_cust_az12

--======== ERP LOC DATA TRANSFORMATION
SELECT 
cid,
cntry
FROM bronze.erp_loc_a101

--remove the "-"
SELECT 
REPLACE(cid, '-', '') cid, --Handling invalid value
cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (select cst_key from silver.crm_cust_info) --confrmation of the 2 table if there is unmatching data

SELECT 
REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101
WHERE cid NOT IN (select cst_key from silver.crm_cust_info) --confirmation of the 2 tables if there is matching data

---DATA TRANSFORMATION TO CHECK FOR THE COUNTRY
SELECT 
DISTINCT --using distinct to figure out the dirty data entry in the column
cntry,
CASE 
    WHEN TRIM(cntry) = 'DE' THEN 'Germany'   --Data normalization
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	When Trim(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

---TRANSFORMED DATA TO BE INSERTED
PRINT '>> Truncating Table: silver.erp_loc_a101'
TRUNCATE TABLE silver.erp_loc_a101
PRINT '>> Inserting Data Into: silver.erp_loc_a101'
INSERT INTO silver.erp_loc_a101(cid, cntry)
SELECT
REPLACE(cid, '-', '') cid,
CASE 
    WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	When Trim(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END cntry
FROM bronze.erp_loc_a101

-----======== ERP CAT DATA TRANSFORMATION
Select 
id,
cat,
subcat,
maintenance
From bronze.erp_px_cat_g1v2

--CHECKING FOR UNWANTED SPACES IN CAT (Use where Function with not equal)
Select * From bronze.erp_px_cat_g1v2
where cat != TRIM(cat)

--CHECKING FOR UNWANTED SPACES IN SUBCAT (Use where Function with not equal)
Select * From bronze.erp_px_cat_g1v2
where subcat != TRIM(subcat)

--CHECKING FOR UNWANTED SPACES IN MAINTENANCE (Use where Function with not equal)
Select * From bronze.erp_px_cat_g1v2
where maintenance != TRIM(maintenance)

--DATA STANDARDIZATION & CONSISTENCY
Select DISTINCT cat From bronze.erp_px_cat_g1v2

Select DISTINCT subcat From bronze.erp_px_cat_g1v2

Select DISTINCT maintenance From bronze.erp_px_cat_g1v2

---TRANSFORMED DATA TO BE INSERTED
PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
TRUNCATE TABLE silver.erp_px_cat_g1v2
PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2'
INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
Select
id,
cat,
subcat,
maintenance
From bronze.erp_px_cat_g1v2

