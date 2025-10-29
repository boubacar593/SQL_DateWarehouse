/* ------------------------------------------------------------
   Procédure : silver.load_silver
   But       : charger/normaliser les données depuis bronze. vers silver.
   Remarques : - CREATE OR ALTER doit être la première instruction du batch
              - Chaque bloc majeur a son TRY/CATCH pour logging et poursuite
              - TRUNCATE peut échouer (FK/permissions) -> fallback DELETE
------------------------------------------------------------ */
SET NOCOUNT ON;
GO

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    ----------------------------------------------------------------
    -- 1) CLIENTS : vidage + insert avec déduplication (dernier enregistrement)
    ----------------------------------------------------------------
    BEGIN TRY
        PRINT '--- CLIENTS : début ---';

        -- Tentative de TRUNCATE (rapide). Si échec, fallback en DELETE.
        BEGIN TRY
            TRUNCATE TABLE silver.crm_cust_info;
            PRINT 'silver.crm_cust_info TRUNCATE OK';
        END TRY
        BEGIN CATCH
            PRINT 'TRUNCATE silver.crm_cust_info a échoué, fallback to DELETE. Erreur: ' + ERROR_MESSAGE();
            DELETE FROM silver.crm_cust_info;
            PRINT 'silver.crm_cust_info DELETE OK';
        END CATCH;

        PRINT 'Insertion des données dans la table silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info(
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_material_status,
            cst_gndr,
            cst_create_date
        )
        SELECT 
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname)  AS cst_lastname,
            CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
                 WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                 ELSE 'N/A'
            END AS cst_material_status,
            CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                 ELSE 'N/A'
            END AS cst_gndr,
            cst_create_date
        FROM (
            -- garder la ligne la plus récente par cst_id
            SELECT *,
                ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
        ) t
        WHERE flag_last = 1;

        PRINT 'CLIENTS : insertion terminée';
    END TRY
    BEGIN CATCH
        PRINT 'ERREUR dans la section CLIENTS : ' + ERROR_MESSAGE();
        -- on continue l'exécution
    END CATCH;


    ----------------------------------------------------------------
    -- 2) PRODUITS : vidage + insert + conversion des dates et calcul prd_end_dt
    ----------------------------------------------------------------
    BEGIN TRY
        PRINT '--- PRODUITS : début ---';

        BEGIN TRY
            TRUNCATE TABLE silver.crm_prd_info;
            PRINT 'silver.crm_prd_info TRUNCATE OK';
        END TRY
        BEGIN CATCH
            PRINT 'TRUNCATE silver.crm_prd_info a échoué, fallback to DELETE. Erreur: ' + ERROR_MESSAGE();
            DELETE FROM silver.crm_prd_info;
            PRINT 'silver.crm_prd_info DELETE OK';
        END CATCH;

        PRINT 'Insertion des données dans la table silver.crm_prd_info';

        /*
          Logique:
          - prd_start_dt est stocké en INT (YYYYMMDD) -> TRY_CONVERT(date, CONVERT(varchar(8), ..., 112))
          - prd_end_dt calculé comme (lead(prd_start_dt) - 1 jour) pour chaque prd_key
          - Si la conversion échoue, TRY_CONVERT renverra NULL (sécurisé)
        */

        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_key,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt,
            dwh_create_date
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_key,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Montain'
                WHEN 'R' THEN 'Road'
                WHEN 'T' THEN 'Touring'
                WHEN 'S' THEN 'Others Sales'
                ELSE 'N/A'
            END AS prd_line,
            -- conversion sûre de l'int YYYYMMDD -> date
            TRY_CONVERT(date, CONVERT(varchar(8), prd_start_dt), 112) AS prd_start_dt,
            -- prd_end_dt = date de la ligne suivante (par prd_key) - 1 jour
            DATEADD(
                DAY,
                -1,
                TRY_CONVERT(
                    date,
                    CONVERT(varchar(8), LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)),
                    112
                )
            ) AS prd_end_dt,
            GETDATE() AS dwh_create_date
        FROM bronze.crm_prd_info;

        PRINT 'PRODUITS : insertion terminée';
    END TRY
    BEGIN CATCH
        PRINT 'ERREUR dans la section PRODUITS : ' + ERROR_MESSAGE();
    END CATCH;


    ----------------------------------------------------------------
    -- 3) VENTES : création table temporisée si nécessaire + insert
    ----------------------------------------------------------------
    BEGIN TRY
        PRINT '--- VENTES : début ---';

        -- Drop si existe (pour garantir création propre)
        IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
            DROP TABLE silver.crm_sales_details;

        CREATE TABLE silver.crm_sales_details(
            sls_ord_num NVARCHAR(50),
            sls_prd_key NVARCHAR(50),
            sls_cust_id NVARCHAR(50),
            sls_order_dt DATE,
            sls_ship_dt DATE,
            sls_due_dt DATE,
            sls_sales FLOAT,
            sls_quantity INT,
            sls_price FLOAT,
            dwh_create_date DATETIME2 DEFAULT GETDATE()
        );

        TRUNCATE TABLE silver.crm_sales_details;

        PRINT 'Insertion des données dans la table silver.crm_sales_details';

        INSERT INTO silver.crm_sales_details(
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            -- conversion sécurisée si int YYYYMMDD attendu
            CASE WHEN sls_order_dt = 0 OR LEN(CONVERT(varchar(50), sls_order_dt)) != 8 THEN NULL
                 ELSE TRY_CONVERT(date, CONVERT(varchar(8), sls_order_dt), 112)
            END AS sls_order_dt,

            CASE WHEN sls_ship_dt = 0 OR LEN(CONVERT(varchar(50), sls_ship_dt)) != 8 THEN NULL
                 ELSE TRY_CONVERT(date, CONVERT(varchar(8), sls_ship_dt), 112)
            END AS sls_ship_dt,

            CASE WHEN sls_due_dt = 0 OR LEN(CONVERT(varchar(50), sls_due_dt)) != 8 THEN NULL
                 ELSE TRY_CONVERT(date, CONVERT(varchar(8), sls_due_dt), 112)
            END AS sls_due_dt,

            CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,

            sls_quantity,

            CASE WHEN sls_price IS NULL OR sls_price <= 0
                THEN CASE WHEN sls_quantity = 0 THEN NULL ELSE sls_sales / NULLIF(sls_quantity,0) END
                ELSE sls_price
            END AS sls_price

        FROM bronze.crm_sales_details;

        PRINT 'VENTES : insertion terminée';
    END TRY
    BEGIN CATCH
        PRINT 'ERREUR dans la section VENTES : ' + ERROR_MESSAGE();
    END CATCH;


    ----------------------------------------------------------------
    -- 4) ERP CUSTOM AZ12 : insert + nettoyage / standardisation
    ----------------------------------------------------------------
    BEGIN TRY
        PRINT '--- ERP_CUST_AZ12 : début ---';

        BEGIN TRY
            TRUNCATE TABLE silver.erp_cust_az12;
            PRINT 'silver.erp_cust_az12 TRUNCATE OK';
        END TRY
        BEGIN CATCH
            PRINT 'TRUNCATE silver.erp_cust_az12 a échoué, fallback to DELETE. Erreur: ' + ERROR_MESSAGE();
            DELETE FROM silver.erp_cust_az12;
            PRINT 'silver.erp_cust_az12 DELETE OK';
        END CATCH;

        PRINT 'Insertion des données dans la table silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12(
            cid,
            bdate,
            gen
        )
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid)) ELSE cid END AS cid,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate,
            CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                 ELSE 'N/A'
            END AS gen
        FROM bronze.erp_cust_az12;

        PRINT 'ERP_CUST_AZ12 : insertion terminée';
    END TRY
    BEGIN CATCH
        PRINT 'ERREUR dans la section ERP_CUST_AZ12 : ' + ERROR_MESSAGE();
    END CATCH;


    ----------------------------------------------------------------
    -- 5) LOCATIONS : insert / nettoyage
    ----------------------------------------------------------------
    BEGIN TRY
        PRINT '--- LOCATIONS : début ---';

        BEGIN TRY
            TRUNCATE TABLE silver.erp_loc_a101;
            PRINT 'silver.erp_loc_a101 TRUNCATE OK';
        END TRY
        BEGIN CATCH
            PRINT 'TRUNCATE silver.erp_loc_a101 a échoué, fallback to DELETE. Erreur: ' + ERROR_MESSAGE();
            DELETE FROM silver.erp_loc_a101;
            PRINT 'silver.erp_loc_a101 DELETE OK';
        END CATCH;

        PRINT 'Insertion des données dans la table silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-', '') AS cid,
            CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
                 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
                 ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;

        PRINT 'LOCATIONS : insertion terminée';
    END TRY
    BEGIN CATCH
        PRINT 'ERREUR dans la section LOCATIONS : ' + ERROR_MESSAGE();
    END CATCH;


    ----------------------------------------------------------------
    -- 6) CATEGORIES : simple copy
    ----------------------------------------------------------------
    BEGIN TRY
        PRINT '--- CATEGORIES : début ---';

        BEGIN TRY
            TRUNCATE TABLE silver.erp_px_cat_g1v2;
            PRINT 'silver.erp_px_cat_g1v2 TRUNCATE OK';
        END TRY
        BEGIN CATCH
            PRINT 'TRUNCATE silver.erp_px_cat_g1v2 a échoué, fallback to DELETE. Erreur: ' + ERROR_MESSAGE();
            DELETE FROM silver.erp_px_cat_g1v2;
            PRINT 'silver.erp_px_cat_g1v2 DELETE OK';
        END CATCH;

        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance
        FROM bronze.erp_px_cat_g1v2;

        PRINT 'CATEGORIES : insertion terminée';
    END TRY
    BEGIN CATCH
        PRINT 'ERREUR dans la section CATEGORIES : ' + ERROR_MESSAGE();
    END CATCH;


    ----------------------------------------------------------------
    -- Fin de la procédure
    ----------------------------------------------------------------
    PRINT '--- load_silver terminé ---';
END;
GO

/* Pour exécuter la procédure après création : */
EXEC silver.load_silver;
GO
