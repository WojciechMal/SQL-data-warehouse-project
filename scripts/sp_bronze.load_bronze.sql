CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @start_time_procedure DATETIME, @end_time_procedure DATETIME;
	BEGIN TRY
		SET @start_time_procedure = GETDATE();
		PRINT '===============================';
		PRINT 'Load Bronze Layer';
		PRINT '===============================';
	
		PRINT '-------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------';

		PRINT '>> Truncating Table: [bronze].[crm_cust_info]'
		SET @start_time = GETDATE();
		TRUNCATE TABLE [bronze].[crm_cust_info]

		PRINT '>> Inserting Data Into: [bronze].[crm_cust_info]'
		BULK INSERT [bronze].[crm_cust_info]
		FROM 'C:\Users\wojciech.malinowski_\Desktop\Projects\dwh\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
		PRINT '--------------------------------'

		PRINT '>> Truncating Table: [bronze].[crm_prd_info]'
		SET @start_time = GETDATE();
		TRUNCATE TABLE [bronze].[crm_prd_info]
		PRINT '>> Inserting Data Into: [bronze].[crm_prd_info]'
		BULK INSERT [bronze].[crm_prd_info]
		FROM 'C:\Users\wojciech.malinowski_\Desktop\Projects\dwh\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
		PRINT '--------------------------------'

		PRINT '>> Truncating Table: [bronze].[crm_sales_details]'
		SET @start_time = GETDATE();
		TRUNCATE TABLE [bronze].[crm_sales_details]
		PRINT '>> Inserting Data Into: [bronze].[crm_sales_details]'
		BULK INSERT [bronze].[crm_sales_details]
		FROM 'C:\Users\wojciech.malinowski_\Desktop\Projects\dwh\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
		PRINT '--------------------------------'


		PRINT '-------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------';

		PRINT '>> Truncating Table: [bronze].[erp_cust_az12]'
		SET @start_time = GETDATE();
		TRUNCATE TABLE [bronze].[erp_cust_az12]
		PRINT '>> Inserting Data Into: [bronze].[erp_cust_az12]'
		BULK INSERT [bronze].[erp_cust_az12]
		FROM 'C:\Users\wojciech.malinowski_\Desktop\Projects\dwh\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
		PRINT '--------------------------------'

		PRINT '>> Truncating Table: [bronze].[erp_loc_a101]'
		SET @start_time = GETDATE();
		TRUNCATE TABLE [bronze].[erp_loc_a101]
		PRINT '>> Inserting Data Into: [bronze].[erp_loc_a101]'
		BULK INSERT [bronze].[erp_loc_a101]
		FROM 'C:\Users\wojciech.malinowski_\Desktop\Projects\dwh\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
		PRINT '--------------------------------'

		PRINT '>> Truncating Table: [bronze].[erp_px_cat_g1v2]'
		SET @start_time = GETDATE();
		TRUNCATE TABLE [bronze].[erp_px_cat_g1v2]
		PRINT '>> Inserting Data Into: [bronze].[erp_px_cat_g1v2]'
		BULK INSERT [bronze].[erp_px_cat_g1v2]
		FROM 'C:\Users\wojciech.malinowski_\Desktop\Projects\dwh\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
		PRINT '--------------------------------'

		SET @end_time_procedure = GETDATE();
		PRINT '>> Procedure Load Duration: ' + CAST(DATEDIFF(second, @start_time_procedure, @end_time_procedure) as NVARCHAR) + ' seconds';
		PRINT '--------------------------------'

	END TRY
	BEGIN CATCH
		PRINT '=====================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=====================================';
	END CATCH
END
