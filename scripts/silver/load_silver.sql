/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
   Declare @start_time Datetime, @end_time datetime, @batch_start_time Datetime, @batch_end_time Datetime
    BEGIN TRY
	    Set @batch_start_time = GETDATE()
		PRINT '================================================================='
		PRINT 'Loading Silver Layer'
		PRINT '================================================================='

		PRINT '------------------------------------------------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '------------------------------------------------------------------'
	
		--CRM CUST DATA
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
			WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single' --This is where the alphabet is ina  smaller letter or there is space in front of the alphabet
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
		)t where flag_last = 1
		Set @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'

		--CRM PRD DATA
		Set @start_time = GETDATE()
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
			  ,REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') AS cat_id-- SUBSTRING() Extracts specific part of a string value / derive new column
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
		  Set @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @end_time) AS NVARCHAR) + ' seconds' --the duration of loading
		PRINT '----------------------------------------------------------------------------'


		--CRM SALES DATA
		Set @start_time = GETDATE()
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
		Set @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'



		--ERP CUST DATA
		PRINT '------------------------------------------------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '------------------------------------------------------------------'
	
		Set @start_time = GETDATE()
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
		Set @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'


		--ERP LOC DATA
		Set @start_time = GETDATE()
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
		Set @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'


		--ERP CAT DATA
		Set @start_time = GETDATE()
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
		Set @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'
		Set @batch_end_time = GETDATE()
		PRINT '================================================================================='
		PRINT 'Loading Silver Layer is completed'
		PRINT '--Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds'
		PRINT '================================================================================='
	END TRY
	BEGIN CATCH
	     --Monitoring any error when loading the data
		 --Track ETL Duration - Helps to identify bottlenecks, optimize performance, monitor trends, detect issues
		 PRINT'========================================================================'
		 PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER'
		 PRINT 'Error Message' + ERROR_MESSAGE()
		 PRINT 'Error Message' + Cast (ERROR_Number() as NVARCHAR)
		 PRINT 'Error Message' + Cast (ERROR_STATE() as NVARCHAR)
		 PRINT'========================================================================'
	END CATCH
END
