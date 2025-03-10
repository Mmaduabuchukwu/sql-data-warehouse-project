/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
--- LOAD DATA INTO BRONZE DATABASE USING "BULK INSERT"
/* Add TRY..CATCH - Ensures error handling, data integrity, and  issue logging for easier debugging
The TRY, SQL runs the TRY block, and if it fails, it runs the CATCH block to handle the error*/
Create or Alter Procedure bronze.load_bronze AS     --Build Bronze Layer, Create Stored Procedure
BEGIN
    Declare @start_time Datetime, @end_time datetime, @batch_start_time Datetime, @batch_end_time Datetime
    BEGIN TRY
	    Set @batch_start_time = GETDATE()
		PRINT '================================================================='
		PRINT 'Loading Bronze Layer'
		PRINT '================================================================='

		PRINT '------------------------------------------------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '------------------------------------------------------------------'
	
		Set @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.crm_cust_info' 
		Truncate Table bronze.crm_cust_info

		PRINT '>> Inserting Data Into: bronze.crm_cust_info' 
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\cpdon\Downloads\Analyst_Project\Braa sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			  FIRSTROW = 2,
			  FIELDTERMINATOR = ',',
			  TABLOCK 
		)
		Set @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'

		Set @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.crm_prd_info'
		Truncate Table bronze.crm_prd_info

		PRINT '>> Inserting Data Into: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\cpdon\Downloads\Analyst_Project\Braa sql-data-warehouse-project\datasets\source_crm\Prd_info.csv'
		WITH (
			  FIRSTROW = 2,
			  FIELDTERMINATOR = ',',
			  TABLOCK 
		)
		Set @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @end_time) AS NVARCHAR) + ' seconds' --the duration of loading
		PRINT '----------------------------------------------------------------------------'

		Set @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.crm_sales_details'
		Truncate Table bronze.crm_sales_details

		PRINT '>> Inserting Data Into: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\cpdon\Downloads\Analyst_Project\Braa sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			  FIRSTROW = 2,
			  FIELDTERMINATOR = ',',
			  TABLOCK 
		)
		Set @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'

		PRINT '------------------------------------------------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '------------------------------------------------------------------'
	
		Set @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_loc_a101'
		Truncate Table bronze.erp_loc_a101

		PRINT '>> Inserting Data Into: erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\cpdon\Downloads\Analyst_Project\Braa sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			  FIRSTROW = 2,
			  FIELDTERMINATOR = ',',
			  TABLOCK 
		)
		Set @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'

		Set @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_cust_az12'
		Truncate Table bronze.erp_cust_az12

		PRINT '>> Inserting Data Into: erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\cpdon\Downloads\Analyst_Project\Braa sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			  FIRSTROW = 2,
			  FIELDTERMINATOR = ',',
			  TABLOCK 
		)
		Set @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'

		Set @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2'
		Truncate Table bronze.erp_px_cat_g1v2

		PRINT '>> Inserting Data Into: erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\cpdon\Downloads\Analyst_Project\Braa sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			  FIRSTROW = 2,
			  FIELDTERMINATOR = ',',
			  TABLOCK 
		)
		Set @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'
		Set @batch_end_time = GETDATE()
		PRINT '================================================================================='
		PRINT 'Loading Bronze Layer is completed'
		PRINT '--Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds'
		PRINT '================================================================================='
	END TRY
	BEGIN CATCH
	     --Monitoring any error when loading the data
		 --TRack ETL Duration - Helps to identify botterecks, optimize performance, monitor trends, detect issues
		 PRINT'========================================================================'
		 PRINT 'ERROR OCCURED DURING LODING BRONZE LAYER'
		 PRINT 'Error Message' + ERROR_MESSAGE()
		 PRINT 'Error Message' + Cast (ERROR_Number() as NVARCHAR)
		 PRINT 'Error Message' + Cast (ERROR_STATE() as NVARCHAR)
		 PRINT'========================================================================'
	END CATCH
END
