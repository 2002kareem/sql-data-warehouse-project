/*
=======================================================================================
Stored Procedure : Load Bronze Layer (Source -> Bronze)
=======================================================================================
Script Purpose :
	This stored procedure loads data into the bronze schema from external CSV files .
	It performing the following actions :

	-Truncate the bronze tables before loading data . 
	-Uses the 'BULK INSERT' command to load data from csv files to bronze tables . 


Parameters :
	None .
	This stored procedure dose not accept any parameters or return any values .

Usage Example :
	EXEC bronze.load_bronze
=======================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @Start_time DATETIME  , @End_time DATETIME , @Batch_Start_Time DATETIME , @Batch_End_Time DATETIME
	BEGIN TRY
	SET @Batch_Start_Time = GETDATE()
		PRINT '================================================================================================='
		PRINT '										Loading Bronze Layer'
		PRINT '================================================================================================='

		PRINT '*************************************************************************************************'
		PRINT '*************************************************************************************************'


		PRINT '-------------------------------------------------------------------------------------------------'
		PRINT '										Loading CRM Tables'
		PRINT '-------------------------------------------------------------------------------------------------'


		SET @Start_time = GETDATE()

		PRINT '>> Truncating Table: bronze.crm_cust_info'

		TRUNCATE TABLE bronze.crm_cust_info

		PRINT '>> Inserting Data Into : bronze.crm_cust_info'

		BULK INSERT bronze.crm_cust_info 
		FROM 'E:\Data analysis\Data Warehouse\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH  (
			FIRSTROW = 2 , 
			FIELDTERMINATOR = ',' , 
			TABLOCK
		)

		SET @End_time = GETDATE()
		PRINT '>> Load Duration : ' + CAST( DATEDIFF(SECOND , @Start_time , @End_time) AS NVARCHAR(50)) + ' Seconds .'
		PRINT '>> ##############################################################################################'

		SET @Start_time = GETDATE()

		PRINT '>> Truncating Table: bronze.crm_prd_inf'

		TRUNCATE TABLE bronze.crm_prd_info 

		PRINT '>> Inserting Data Into : bronze.crm_prd_info'

		BULK INSERT bronze.crm_prd_info
		FROM 'E:\Data analysis\Data Warehouse\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2 , 
			FIELDTERMINATOR = ',' ,
			TABLOCK
		)

		SET @End_time = GETDATE()
		PRINT '>> Load Duration : ' + CAST( DATEDIFF(SECOND , @Start_time , @End_time) AS NVARCHAR(50)) + ' Seconds .'
		PRINT '>> ##############################################################################################'

		SET @Start_time = GETDATE()

		PRINT '>> Truncating Table: bronze.crm_sales_details'

		TRUNCATE TABLE bronze.crm_sales_details

		PRINT '>> Inserting Data Into : bronze.crm_sales_details'

		BULK INSERT bronze.crm_sales_details
		FROM 'E:\Data analysis\Data Warehouse\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2 , 
			FIELDTERMINATOR = ',' , 
			TABLOCK
		)

		SET @End_time = GETDATE()
		PRINT '>> Load Duration : ' + CAST( DATEDIFF(SECOND , @Start_time , @End_time) AS NVARCHAR(50)) + ' Seconds .'
		PRINT '>> ##############################################################################################'


		PRINT '-------------------------------------------------------------------------------------------------'
		PRINT '										Loading ERP Tables'
		PRINT '-------------------------------------------------------------------------------------------------'



		SET @Start_time = GETDATE()

		PRINT '>> Truncating Table: bronze.erp_cust_az12'

		TRUNCATE TABLE bronze.erp_cust_az12

		PRINT '>> Inserting Data Into : bronze.erp_cust_az12'

		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\Data analysis\Data Warehouse\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW  = 2 , 
			FIELDTERMINATOR  = ',' , 
			TABLOCK 
		)

		SET @End_time = GETDATE()
		PRINT '>> Load Duration : ' + CAST( DATEDIFF(SECOND , @Start_time , @End_time) AS NVARCHAR(50)) + ' Seconds .'
		PRINT '>> ##############################################################################################'

		SET @Start_time = GETDATE()

		PRINT '>> Truncating Table: bronze.erp_loc_a101'

		TRUNCATE TABLE bronze.erp_loc_a101

		PRINT '>> Inserting Data Into : bronze.erp_loc_a101'

		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\Data analysis\Data Warehouse\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2 , 
			FIELDTERMINATOR = ',' , 
			TABLOCK
		)


		SET @End_time = GETDATE()
		PRINT '>> Load Duration : ' + CAST( DATEDIFF(SECOND , @Start_time , @End_time) AS NVARCHAR(50)) + ' Seconds .'
		PRINT '>> ##############################################################################################'

		SET @Start_time = GETDATE()

		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2'

		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		PRINT '>> Inserting Data Into : bronze.erp_px_cat_g1v2'

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\Data analysis\Data Warehouse\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2 , 
			FIELDTERMINATOR = ',' , 
			TABLOCK
		)

		SET @End_time = GETDATE()
		PRINT '>> Load Duration : ' + CAST( DATEDIFF(SECOND , @Start_time , @End_time) AS NVARCHAR(50)) + ' Seconds .'
		PRINT '>> ##############################################################################################'
		SET @Batch_End_Time = GETDATE()

		PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'
		PRINT '										Loading Bronze Layer Is Completed'
		PRINT '										-Total Load Duration : ' + CAST( DATEDIFF(SECOND , @Batch_Start_Time , @Batch_End_Time ) AS NVARCHAR(50)) + ' Seconds . '
		PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'

	END TRY
	BEGIN CATCH
		PRINT '================================================================================================='
		PRINT '										ERROR OCCURED DURING BRONZE LAYER'
		PRINT '										Error Message' + ERROR_MESSAGE()
		PRINT '										Error Number' + CAST( ERROR_NUMBER() AS NVARCHAR(50) )
		PRINT '										Error State' + CAST( ERROR_STATE() AS NVARCHAR(50) )
		PRINT '================================================================================================='
	END CATCH

END
