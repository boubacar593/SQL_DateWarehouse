/******************************************************************************************
    OBJECTIF GÉNÉRAL :
    Ce script SQL effectue des vérifications de qualité de données après les jointures 
    entre différentes sources (SILVER) et les vues créées dans le schéma GOLD.
    
    Étapes principales :
    1. Vérifier les doublons de clients après la jointure des tables sources.
    2. Contrôler la cohérence du genre (sexe) entre les deux sources de données.
    3. Consulter les vues de dimension et de faits créées dans le schéma GOLD.
    4. Vérifier la distribution des genres dans la table gold.dim_customers.
******************************************************************************************/


/**********************************************
************ 1. VÉRIFICATION DES DOUBLONS *****
**********************************************/

-- Objectif : vérifier si un même client (cst_id) apparaît plusieurs fois 
-- après la jointure entre les trois sources (crm_cust_info, erp_cust_az12, erp_loc_a101).
-- Si COUNT(*) > 1, cela indique un doublon à analyser (mauvaise clé ou jointure non unique).

SELECT 
    cst_id, 
    COUNT(*) AS nombre_d_occurrences
FROM (
    SELECT 
        ci.cst_id,                  -- Identifiant client
        ci.cst_key,                 -- Clé client interne
        ci.cst_firstname,           -- Prénom
        ci.cst_material_status,     -- Statut marital
        ci.cst_gndr,                -- Genre dans la première source (CRM)
        ca.bdate,                   -- Date de naissance dans la deuxième source
        ca.gen,                     -- Genre dans la deuxième source (ERP)
        la.cntry                    -- Pays (localisation)
    FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid
) t
GROUP BY cst_id
HAVING COUNT(*) > 1;  -- Affiche uniquement les clients présents plusieurs fois



/**********************************************
************ 2. CONTRÔLE DE COHÉRENCE DU GENRE *****
**********************************************/

-- Objectif : comparer le genre provenant des deux sources de données.
-- Règle : 
--   - Si le genre dans CRM (ci.cst_gndr) est différent de 'N/A', on le garde.
--   - Sinon, on prend celui de la source ERP (ca.gen), ou 'N/A' s’il est absent.
-- Ce test permet de s’assurer que la logique du CASE utilisée dans la vue dim_customers est correcte.

SELECT DISTINCT
    ci.cst_gndr AS genre_crm,       -- Genre depuis la source CRM
    ca.gen AS genre_erp,            -- Genre depuis la source ERP
    CASE 
        WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'N/A')
    END AS genre_final               -- Genre final après application de la règle
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid;



/**********************************************
************ 3. CONSULTATION DES VUES GOLD *****
**********************************************/

-- Ces sélections permettent de vérifier que les vues du modèle en étoile (Data Warehouse)
-- ont bien été créées et contiennent les données attendues.

SELECT * FROM gold.dim_customers;   -- Vue de la dimension clients
SELECT * FROM gold.dim_products;    -- Vue de la dimension produits
SELECT * FROM gold.fact_sales;      -- Vue des faits de ventes



/**********************************************
************ 4. ANALYSE DU CHAMP "GENDER" *****
**********************************************/

-- Objectif : vérifier les valeurs distinctes du champ "gender" dans la dimension clients.
-- Cela permet de contrôler la qualité du nettoyage du champ (par ex. N/A, M, F, etc.).

SELECT DISTINCT gender 
FROM gold.dim_customers;
