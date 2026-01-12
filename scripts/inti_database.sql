/*========================================================
  DATA WAREHOUSE SETUP
  Database + Medallion Architecture Schemas
========================================================*/

-- Drop database if it already exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'datawarehouse')
BEGIN
    ALTER DATABASE datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE datawarehouse;
END;
GO

-- Create database
CREATE DATABASE datawarehouse;
GO

-- Use database
USE datawarehouse;
GO

/*===========================
  Create Schemas
===========================*/

-- Bronze (Raw ingestion layer)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

-- Silver (Cleansed & conformed layer)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

-- Gold (Analytics & business layer)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO
