/*
=============================================
Create Database and Schemas
=============================================
Script Purpose:
    This script creates a new database named 'Datawarehouse' after checking if it already exists. If the database exists, it is
    dropped and recreated. Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'Datawarehouse' database if it exists. All the data will be permanently deleted.
    Proceed with caution and ensure you have proper backups before running the script.
*/

USE master;
GO
  
--Drop and recreate the 'Datawarehouse' Database
  IF EXISTS (Select 1 FROM sys.databases WHERE name= 'Datawarehouse')
  BEGIN
      ALTER DATABASE Datawarehouse SET SINGLE_USER ROLLBACK IMMEDIATE;
      DROP DATABASE Datawarehouse;
      END;
      GO

-- Create DATABASE Datawarehouse;

USE datawarehouse;
GO

  -- Create schemas

Create schema bronze;
GO
  
Create schema silver;
GO
  
Create schema gold;
GO
