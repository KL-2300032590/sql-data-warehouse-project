
/*
Bronze-layer load pipeline that wipes and reloads all CRM and ERP staging tables from CSV files via BULK INSERT.
It timestamps and logs how long each table and the full batch take, giving ops a clean run-rate view.
The stored procedure bronze.load_bronze is a compiled, 
reusable database object that packages this entire ingestion workflow into one callable unit.
EXEC
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

        /* CRM CUSTOMER */
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM '/Users/pardhasaradhireddy/Downloads/projects/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'crm_cust_info loaded in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' sec';

        /* CRM PRODUCT */
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM '/Users/pardhasaradhireddy/Downloads/projects/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'crm_prd_info loaded in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' sec';

        /* CRM SALES */
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM '/Users/pardhasaradhireddy/Downloads/projects/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'crm_sales_details loaded in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' sec';

        ---------------------------------------------------
        -- ERP TABLES
        ---------------------------------------------------
        PRINT '>> Loading ERP tables';

        /* ERP CUSTOMER */
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM '/Users/pardhasaradhireddy/Downloads/projects/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'erp_cust_az12 loaded in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' sec';

        /* ERP LOCATION */
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM '/Users/pardhasaradhireddy/Downloads/projects/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'erp_loc_a101 loaded in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' sec';

        /* ERP PRICE CATALOG */
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/Users/pardhasaradhireddy/Downloads/projects/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'erp_px_cat_g1v2 loaded in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' sec';

        ---------------------------------------------------
        -- BATCH METRICS
        ---------------------------------------------------
        SET @batch_end_time = GETDATE();

        PRINT '====================================================';
        PRINT 'BRONZE LOAD COMPLETED';
        PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' sec';
        PRINT '====================================================';
    END TRY
    BEGIN CATCH
        PRINT '====================================================';
        PRINT 'BRONZE LOAD FAILED';
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR);
        PRINT '====================================================';

        THROW;   -- surfaces the error to schedulers, ADF, etc.
    END CATCH
END;
