/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Drop and recreate the 'DataWareHouse' database
IF Exists (Select 1 from sys.databases Where name = 'DataWarehouse')
BEGIN
     ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	 DROP DATABASE DateWarehouse;
end;

--- Create the 'DatWarehouse' dataset
Create DataBase DataWarehouse

Use DataWarehouse

--Create schema
Create Schema bronze
  
Create Schema silver;
  
Create Schema gold;
