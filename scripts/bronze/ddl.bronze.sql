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

/* Building the Bronze Layer for crm and erp details*/
IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
   DROP TABLE bronze.crm_cust_info;

Create Table bronze.crm_cust_info (
cst_id INT,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr Nvarchar(50),
cst_create_date DATE
)


IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
   DROP TABLE bronze.crm_prd_info;
Create Table bronze.crm_prd_info (
prd_id INT,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost Int,
prd_line nvarchar(50),
prd_start_dt Datetime,
prd_end_date Datetime
)


IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
   DROP TABLE bronze.crm_sales_details;
Create Table bronze.crm_sales_details (
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id iNT,
sls_order_dt Int,
sls_ship_dt Int,
sls_due_dt Int,
sls_sales Int,
sls_quantity Int,
sls_Price int
)

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
   DROP TABLE bronze.erp_loc_a101;
Create Table bronze.erp_loc_a101(
cid nvarchar(50),
cntry nvarchar(50)
)

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
   DROP TABLE bronze.erp_cust_az12;
Create Table bronze.erp_cust_az12
(
cid Nvarchar(50),
bdate Date,
gen nvarchar(50)
)


IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
   DROP TABLE bronze.erp_px_cat_g1v2;
Create Table bronze.erp_px_cat_g1v2
(
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50)
)
