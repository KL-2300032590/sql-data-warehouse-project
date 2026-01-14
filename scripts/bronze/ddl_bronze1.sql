/*
===============================================================================
Stored Procedure: bronze.load_bronze
Purpose:
- Truncate and reload all Bronze-layer CRM and ERP tables from CSV files
- Uses Docker-mounted datasets folder (/datasets)
- Designed for local + production-style ETL learning
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @start_time        DATETIME,
        @end_time          DATETIME,
        @batch_start_time  DATETIME,
        @batch_end_time    DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '====================================================';
        PRINT 'BRONZE LOAD PIPELINE - STARTED';
        PRINT '====================================================';

        ---------------------------------------------------
        -- CRM TABLES
        ---------------------------------------------------
        PRINT '>> Loading CRM tables';

        /* =================================================
           CRM CUSTOMER
        ================================================= */
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM '/data/source_crm/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'crm_cust_info loaded in ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' sec';

        /* =================================================
           CRM PRODUCT
        ================================================= */
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM '/data/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'crm_prd_info loaded in ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' sec';

        /* =================================================
           CRM SALES
        ================================================= */
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM '/data/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'crm_sales_details loaded in ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' sec';

        ---------------------------------------------------
        -- ERP TABLES
        ---------------------------------------------------
        PRINT '>> Loading ERP tables';

        /* =================================================
           ERP CUSTOMER
        ================================================= */
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM '/data/source_erp/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'erp_cust_az12 loaded in ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' sec';

        /* =================================================
           ERP LOCATION
        ================================================= */
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM '/data/source_erp/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'erp_loc_a101 loaded in ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' sec';

        /* =================================================
           ERP PRICE CATALOG
        ================================================= */
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/data/source_erp/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'erp_px_cat_g1v2 loaded in ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' sec';

        ---------------------------------------------------
        -- BATCH METRICS
        ---------------------------------------------------
        SET @batch_end_time = GETDATE();

        PRINT '====================================================';
        PRINT 'BRONZE LOAD COMPLETED SUCCESSFULLY';
        PRINT 'Total Duration: ' 
              + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' sec';
        PRINT '====================================================';
    END TRY
    BEGIN CATCH
        PRINT '====================================================';
        PRINT 'BRONZE LOAD FAILED';
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Line   : ' + CAST(ERROR_LINE() AS VARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO
