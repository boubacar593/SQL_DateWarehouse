-- ===============================================================
-- Script de création du Data Warehouse
-- Objectif : Créer une base de données DataWarehouse avec les schémas
--             bronze, silver et gold selon les bonnes pratiques.
-- ===============================================================

-- 1️ On se place dans la base 'master' (base système principale)
USE master;
GO

-- 2️ Vérifier si la base 'DataWarehouse' existe déjà
--    Si elle existe, on la supprime pour repartir sur une base propre
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'DataWarehouse')
BEGIN
    PRINT 'Suppression de la base de données existante : DataWarehouse...';
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
    PRINT 'Base supprimée avec succès.';
END
GO

-- 3️ Création de la nouvelle base de données
PRINT 'Création de la base de données DataWarehouse...';
CREATE DATABASE DataWarehouse;
GO

-- 4️ On sélectionne la base nouvellement créée pour travailler dessus
USE DataWarehouse;
GO

-- 5️ Création des schémas selon la méthodologie Data Lake / Data Warehouse :
--     - bronze : données brutes (staging)
--     - silver : données nettoyées et transformées
--     - gold   : données prêtes pour l’analyse et la visualisation

-- Schéma pour les données brutes
CREATE SCHEMA bronze;
GO

-- Schéma pour les données nettoyées et enrichies
CREATE SCHEMA silver;
GO

-- Schéma pour les données analytiques finales
CREATE SCHEMA gold;
GO

-- 6️ Confirmation finale
PRINT 'Base de données DataWarehouse et schémas créés avec succès !';
