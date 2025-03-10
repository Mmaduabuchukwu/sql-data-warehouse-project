/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/
---BUILDING GOLD LAYER WITH VIEW FUNCTION
-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
Select cst_id, Count(*) FROM -- To check for duplicate in the data using the group by and having 
	(SELECT
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_material_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON        Ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON        ci.cst_key = la.cid
) T GROUP BY cst_id
HAVING COUNT(*) > 1 

--DATA INTERGRATION ON THE DATA BASE OF GENDER COLUM FROM CRM & ERP
/*NULLs ofetn come from joined tables!
NULL appear in the output because there was no match*/

SELECT
		DISTINCT
		cI.cst_gndr,
		ca.gen,
		CASE
		    WHEN ci.cst_gndr !='n/a' THEN ci.cst_gndr --CRM is the Master for gender Info 
			ELSE Coalesce(ca.gen, 'n/a')
		End AS new_gen
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON        Ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON        ci.cst_key = la.cid
ORDER BY 1, 2

============================================================================
**Store procedure
============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;

  CREATE VIEW gold.dim_customers AS /*---Create an object using view*/
SELECT
		ROW_NUMBER() OVER(ORDER BY cst_id) as customer_key, --surrogate key
		ci.cst_id customer_id,
		ci.cst_key customer_number,
		ci.cst_firstname first_name,
		ci.cst_lastname last_name,
		la.cntry country,
		ci.cst_material_status marital_status,
		CASE
		    WHEN ci.cst_gndr !='n/a' THEN ci.cst_gndr --CRM is the Master for gender Info 
			ELSE Coalesce(ca.gen, 'n/a')
		End AS gender,
		ca.bdate birthdate,
		ci.cst_create_date create_date
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON        Ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON        ci.cst_key = la.cid

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
/*If End Date is NULL then it is current info of the product!*/
Select prd_key, COUNT(*) from --Checking for UNIQUENESS in the data
	(SELECT 
		pn.prd_id,
		pn.cat_id,
		pn.prd_key,
		pn.prd_nm,
		prd_cost,
		pn.prd_line,
		pn.prd_start_dt,
		pc.cat,
		pc.subcat,
		pc.maintenance
	FROM silver.crm_prd_info pn
	LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.id
	WHERE prd_end_date IS NULL --filter all historical data
	) t GROUP BY prd_key
	HAVING COUNT(*) >1

============================================================================
**Store procedure
============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;

CREATE VIEW gold.dim_products AS
SELECT 
		ROW_NUMBER() OVER(ORDER BY prd_id) product_key, --Surrogate key
		pn.prd_id product_id,
		pn.prd_key product_number,
		pn.prd_nm product_name,
		pn.cat_id category_id,
		pc.cat category,
		pc.subcat subcategory,
		pc.maintenance,
		prd_cost product_cost,
		pn.prd_line product_line,
		pn.prd_start_dt start_date
	FROM silver.crm_prd_info pn
	LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.id
	WHERE prd_end_date IS NULL

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;

CREATE or ALTER VIEW gold.fact_sales AS
SELECT
sd.sls_ord_num order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt order_date,
sd.sls_ship_dt ship_date,
sd.sls_due_dt due_date,
sd.sls_sales sales,
sd.sls_quantity quantity,
sd.sls_price price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFt JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id







