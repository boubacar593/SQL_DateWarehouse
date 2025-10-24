/*
Cette procédure stockée SQL Server, nommée bronze.load_bronze, permet d’automatiser le chargement des données brutes 
(niveau bronze du Data Warehouse) à partir de plusieurs sources CSV.

Elle vide les tables existantes avant chaque nouveau chargement, importe les données de fichiers CSV situés dans 
des répertoires locaux (source_crm et source_erp), mesure et affiche le temps de chargement de chaque table, 
capture les erreurs pour éviter l’arrêt complet du processus, et fournit un bilan complet du chargement avec 
la durée totale.
*/



CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
    SET @start_time = GETDATE()
    BEGIN TRY
        PRINT '=== DÉBUT DU CHARGEMENT DES DONNÉES ===';
        SET @batch_start_time = GETDATE()
        --=============================
        -- 1️ CRM : Customers
        --=============================

        PRINT 'Chargement de crm_cust_info...';
        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\bouba\OneDrive\Desktop\VacancesPartie\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE()
        PRINT 'Durée du chargement: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'secondes';
        PRINT 'crm_cust_info chargé avec succès.';
    PRINT'=============================='
        SET @start_time = GETDATE()
        --=============================
        -- 2️ CRM : Produits
        --=============================
        PRINT 'Chargement de crm_prd_info...';
        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\bouba\OneDrive\Desktop\VacancesPartie\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE()
        PRINT 'Durée du chargement: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'secondes';
        PRINT 'crm_prd_info chargé avec succès.';
         
        PRINT'=============================='
        SET @start_time = GETDATE()
        --=============================
        -- 3️ CRM : Détails ventes
        --=============================
        PRINT 'Chargement de crm_sales_details...';
        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\bouba\OneDrive\Desktop\VacancesPartie\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE()
        PRINT 'Durée du chargement: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'secondes';
        PRINT 'crm_sales_details chargé avec succès.';

        PRINT'=============================='
        SET @start_time = GETDATE()
        --=============================
        -- 4️ ERP : Clients
        --=============================
        PRINT 'Chargement de erp_cust_az12...';
        TRUNCATE TABLE bronze.erp_cust_az12;
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\bouba\OneDrive\Desktop\VacancesPartie\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE()
        PRINT 'Durée du chargement: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'secondes';
        PRINT 'erp_cust_az12 chargé avec succès.';

        PRINT'=============================='
        SET @start_time = GETDATE()
        --=============================
        -- 5️ ERP : Localisations
        --=============================
        PRINT 'Chargement de erp_loc_a101...';
        TRUNCATE TABLE bronze.erp_loc_a101;
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\bouba\OneDrive\Desktop\VacancesPartie\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE()
        PRINT 'Durée du chargement: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'secondes';
        PRINT 'erp_loc_a101 chargé avec succès.';

        PRINT'=============================='
        SET @start_time = GETDATE()
        --=============================
        -- 6️ ERP : Prix
        --=============================
        PRINT 'Chargement de erp_px_cat_g1v2...';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\bouba\OneDrive\Desktop\VacancesPartie\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE()
        PRINT 'Durée du chargement: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'secondes';
        PRINT 'erp_px_cat_g1v2 chargé avec succès.';


        SET @batch_end_time = GETDATE()
        PRINT 'Durée du chargement de la requette complete: '+ CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'secondes';
        PRINT '=== CHARGEMENT TERMINÉ AVEC SUCCÈS ===';

    END TRY

    BEGIN CATCH
        PRINT '⚠️ ERREUR lors du chargement des données :';
        PRINT ERROR_MESSAGE();
    END CATCH
END;
GO
