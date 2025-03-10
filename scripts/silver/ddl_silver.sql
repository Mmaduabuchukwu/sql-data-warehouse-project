/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
--BUILDING SILVER LAYER -DATA WAREHOUSE

IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL
   DROP TABLE silver.crm_cust_info;
Create Table silver.crm_cust_info (
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_material_status nvarchar(50),
	cst_gndr Nvarchar(50),
	cst_create_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE() --for monitoring the date the data being loaded
)

IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
   DROP TABLE silver.crm_prd_info;
Create Table silver.crm_prd_info (
	prd_id int,
	cat_id nvarchar(50),
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost Int,
	prd_line nvarchar(50),
	prd_start_dt Date,
	prd_end_date Date,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
)


IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
   DROP TABLE silver.crm_sales_details;
Create Table silver.crm_sales_details (
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id iNT,
	sls_order_dt Date,
	sls_ship_dt Date,
	sls_due_dt Date,
	sls_sales Int,
	sls_quantity Int,
	sls_Price int,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
)

IF OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL
   DROP TABLE silver.erp_loc_a101;
Create Table silver.erp_loc_a101(
	cid nvarchar(50),
	cntry nvarchar(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
)

IF OBJECT_ID ('silver.erp_cust_az12', 'U') IS NOT NULL
   DROP TABLE silver.erp_cust_az12;
Create Table silver.erp_cust_az12
(
	cid Nvarchar(50),
	bdate Date,
	gen nvarchar(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
)

IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
   DROP TABLE silver.erp_px_cat_g1v2;
Create Table silver.erp_px_cat_g1v2
(
	id nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintenance nvarchar(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
)
