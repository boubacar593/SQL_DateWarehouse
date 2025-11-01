/******************************************************************************************
    OBJECTIF GÉNÉRAL :
    Ce script SQL crée trois vues (views) dans le schéma GOLD :
    1. gold.dim_customers   →  Dimension clients
    2. gold.dim_products    →  Dimension produits
    3. gold.fact_sales      →  Faits de ventes (table de faits reliant clients et produits)
    
    Ces vues s’appuient sur les tables sources du schéma SILVER (données intermédiaires).
    Chaque vue est reconstruite si elle existe déjà (DROP + CREATE).
******************************************************************************************/

/**********************************************
************** DIMENSION CLIENTS **************
**********************************************/

-- Si la vue gold.dim_customers existe déjà, on la supprime pour la recréer proprement
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

-- Création de la vue des clients
CREATE VIEW gold.dim_customers AS
SELECT 
    -- Génère une clé technique unique (clé de substitution)
    ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,

    -- Champs provenant des différentes sources
    ci.cst_id AS customer_id,                     -- Identifiant du client
    ci.cst_key AS customer_number,                -- Clé client (numéro interne)
    ci.cst_firstname AS first_name,               -- Prénom du client
    la.cntry AS country,                          -- Pays du client
    ci.cst_material_status AS marital_status,     -- Statut marital

    -- Règle de gestion : si le genre est différent de 'N/A' dans la source principale, on le garde.
    -- Sinon, on prend la valeur issue de la table erp_cust_az12 ou 'N/A' si elle est absente.
    CASE 
        WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'N/A')
    END AS gender,

    ca.bdate AS birthdate,                        -- Date de naissance
    ci.cst_create_date AS create_date             -- Date de création du client
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid;



/**********************************************
************** DIMENSION PRODUITS *************
**********************************************/

-- Si la vue gold.dim_products existe déjà, on la supprime
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

-- Création de la vue des produits
CREATE VIEW gold.dim_products AS
SELECT 
    -- Génère une clé technique unique (clé de substitution)
    ROW_NUMBER() OVER (ORDER BY prd_key) AS product_key,

    -- Informations de base sur le produit
    pr.prd_id AS product_id,                      -- Identifiant produit
    pr.prd_key AS product_number,                 -- Clé produit interne
    pr.prd_nm AS product_name,                    -- Nom du produit

    -- Informations de catégorisation
    pr.cat_key AS category_key,                   -- Clé de catégorie
    pc.cat AS category,                           -- Catégorie principale
    pc.subcat AS subcategory,                     -- Sous-catégorie
    pc.maintenance,                               -- Niveau de maintenance

    -- Informations financières et logistiques
    pr.prd_cost AS cost,                          -- Coût du produit
    pr.prd_line AS product_line                   -- Ligne de produit
FROM silver.crm_prd_info pr
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pr.cat_key = pc.id;



/**********************************************
************** TABLE DE FAITS VENTES **********
**********************************************/

-- Si la vue gold.fact_sales existe déjà, on la supprime
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

-- Création de la vue des ventes (table de faits)
CREATE VIEW gold.fact_sales AS
SELECT 
    sa.sls_ord_num AS order_number,               -- Numéro de commande
    pr.product_key,                               -- Clé du produit (depuis la dimension produits)
    cu.customer_key,                              -- Clé du client (depuis la dimension clients)
    
    -- Dates importantes du cycle de vente
    sa.sls_order_dt AS order_date,                -- Date de commande
    sa.sls_ship_dt AS shipping_date,              -- Date d’expédition
    sa.sls_due_dt AS due_date,                    -- Date d’échéance
    
    -- Mesures (indicateurs quantitatifs)
    sa.sls_sales AS sales_amount,                 -- Montant total de la vente
    sa.sls_quantity AS quantity,                  -- Quantité vendue
    sa.sls_price AS price                         -- Prix unitaire
FROM silver.crm_sales_details sa
LEFT JOIN gold.dim_customers cu ON sa.sls_cust_id = cu.customer_key
LEFT JOIN gold.dim_products pr ON sa.sls_prd_key = pr.product_key;
GO
