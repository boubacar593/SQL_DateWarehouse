/**********************************************************************************************
    SCRIPT DE VERIFICATION ET DE NETTOYAGE DES DONNEES
    OBJECTIF : Contrôler la qualité, la cohérence et l’intégrité des données
    COUCHES : BRONZE → SILVER
    DOMAINES : Clients, Produits, Ventes, Localisations
**********************************************************************************************/

---------------------------------------------
-- SECTION 1 : VERIFICATION DES CLIENTS (silver.crm_cust_info)
---------------------------------------------

-- Vérifier les doublons et les identifiants nuls
-- Objectif : S'assurer que la clé primaire cst_id est unique et non nulle
SELECT 
    cst_id,
    COUNT(*) AS occurences
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Supprimer les enregistrements avec cst_id nul
DELETE FROM silver.crm_cust_info 
WHERE cst_id IS NULL;

-- Vérifier qu'il ne reste plus d'identifiants nuls
SELECT cst_id 
FROM silver.crm_cust_info
WHERE cst_id IS NULL;

-- Vérifier les espaces indésirables dans les champs texte
-- Si une valeur est retournée, cela signifie qu'il y a des espaces à nettoyer
SELECT 
    cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Vérifier la standardisation des genres (homogénéité des valeurs)
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;


---------------------------------------------
-- SECTION 2 : VERIFICATION DES PRODUITS (bronze.crm_prd_info)
---------------------------------------------

-- Vérifier les doublons et identifiants nuls
SELECT 
    prd_id AS IDPRODUIT,
    COUNT(*) AS occurences
FROM bronze.crm_prd_info
GROUP BY prd_id 
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Vérifier les espaces inutiles dans les noms de produits
SELECT 
    prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Vérifier les prix nuls ou négatifs
SELECT 
    prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Vérifier la cohérence des lignes produits
SELECT DISTINCT prd_line 
FROM bronze.crm_prd_info;

-- Vérifier la cohérence des dates : la date de fin ne doit pas être inférieure à la date de début
SELECT 
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


---------------------------------------------
-- SECTION 3 : VERIFICATION DES VENTES (bronze.crm_sales_details)
---------------------------------------------

-- Vérifier les espaces indésirables dans les numéros de commande
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- Vérifier la cohérence entre les ventes et les produits
-- (Chaque clé produit dans les ventes doit exister dans la table des produits)
SELECT *
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

-- Vérifier la cohérence entre les ventes et les clients
-- (Chaque client dans les ventes doit exister dans la table des clients)
SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- Vérifier la validité des dates (format AAAAMMJJ)
SELECT 
    NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8;

-- Vérifier s'il existe des dates supérieures à la date actuelle
SELECT 
    sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt > 20260101;

-- Vérifier les dates trop anciennes ou trop futures
SELECT 
    NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt > 20260101 OR sls_order_dt < 19000101;

-- Vérifier la cohérence temporelle des dates : 
-- la date de commande ne doit pas être supérieure à la date d’expédition ou d’échéance
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Vérifier la cohérence financière : ventes = quantité * prix
-- Détecter aussi les valeurs nulles ou négatives
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price AS old_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
   OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


---------------------------------------------
-- SECTION 4 : VERIFICATION DES CLIENTS SECONDAIRES (bronze.erp_cust_az12)
---------------------------------------------

-- Nettoyer les identifiants commençant par le préfixe 'NAS'
SELECT
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4 , LEN(cid))
        ELSE cid
    END AS cid,
    bdate,
    gen
FROM bronze.erp_cust_az12;

-- Vérifier la validité des dates de naissance (pas dans le futur, ni avant 1924)
SELECT DISTINCT
    bdate 
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Vérifier les valeurs possibles du genre
SELECT DISTINCT gen FROM bronze.erp_cust_az12;
SELECT DISTINCT gen FROM silver.erp_cust_az12;


---------------------------------------------
-- SECTION 5 : VERIFICATION DES LOCALISATIONS (bronze.erp_loc_a101)
---------------------------------------------

-- Vérifier les pays disponibles et quelques enregistrements aléatoires
SELECT TOP 5 cid, cntry 
FROM bronze.erp_loc_a101;

-- Vérifier la cohérence des pays
SELECT DISTINCT cntry 
FROM bronze.erp_loc_a101;

---------------------------------------------
-- FIN DU SCRIPT
---------------------------------------------
