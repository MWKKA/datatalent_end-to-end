-- =============================================================================
-- Requêtes d'exploration pour identifier les liens (PK/FK) et préparer le schéma en étoile
-- À exécuter dans DBeaver sur data/local.db
-- =============================================================================
--
-- Avant tout : exécuter "PRAGMA table_info(raw_sirene_etablissement);" et
-- "PRAGMA table_info(raw_sirene_unite_legale);" pour voir les noms exacts des
-- colonnes (siret/siren, codeCommuneEtablissement, denominationUniteLegale, etc.).
-- Adapter les requêtes 6 et 7 si les noms diffèrent (casse, tirets).
--
-- Schéma en étoile cible :
--   - Table de fait : offres (une ligne = une offre).
--   - Dimensions : geo (lieu), entreprise (Sirene), type_contrat, rome (métier), temps.
--   - Clés de liaison : commune_code (offres→geo), siren/entreprise_nom (offres→Sirene).
--
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. STRUCTURE DES TABLES (noms de colonnes exacts)
-- -----------------------------------------------------------------------------

PRAGMA table_info(raw_geo_regions);
PRAGMA table_info(raw_geo_departements);
PRAGMA table_info(raw_geo_communes);
PRAGMA table_info(raw_france_travail_offres);
PRAGMA table_info(raw_sirene_etablissement);
PRAGMA table_info(raw_sirene_unite_legale);


-- -----------------------------------------------------------------------------
-- 2. CARDINALITÉS ET CLÉS (vérifier unicité des PK)
-- -----------------------------------------------------------------------------

-- Régions : code = PK
SELECT 'regions' AS table_name, COUNT(*) AS nb_lignes, COUNT(DISTINCT code) AS nb_distinct_code
FROM raw_geo_regions;

-- Départements : code = PK, code_region = FK
SELECT 'departements' AS table_name, COUNT(*) AS nb_lignes, COUNT(DISTINCT code) AS nb_distinct_code
FROM raw_geo_departements;

-- Offres : id = PK
SELECT 'offres' AS table_name, COUNT(*) AS nb_lignes, COUNT(DISTINCT id) AS nb_distinct_id
FROM raw_france_travail_offres;

-- Sirene établissement : siret = PK (adapter le nom de colonne si besoin : siret, SIRET, etc.)
SELECT 'sirene_etablissement' AS table_name, COUNT(*) AS nb_lignes,
       COUNT(DISTINCT siret) AS nb_distinct_siret
FROM raw_sirene_etablissement;

-- Sirene unité légale : siren = PK
SELECT 'sirene_unite_legale' AS table_name, COUNT(*) AS nb_lignes,
       COUNT(DISTINCT siren) AS nb_distinct_siren
FROM raw_sirene_unite_legale;


-- -----------------------------------------------------------------------------
-- 3. LIEN OFFRES → GÉO (commune)
-- -----------------------------------------------------------------------------
-- Offres ont-elles un code commune (INSEE) qui matche raw_geo_communes ?

SELECT 'offres avec commune_code renseigné' AS lib, COUNT(*) AS nb
FROM raw_france_travail_offres WHERE commune_code IS NOT NULL AND commune_code != '';

SELECT 'offres dont commune_code existe dans geo_communes' AS lib, COUNT(*) AS nb
FROM raw_france_travail_offres o
WHERE o.commune_code IS NOT NULL AND o.commune_code != ''
  AND EXISTS (SELECT 1 FROM raw_geo_communes c WHERE c.code = o.commune_code);

-- Exemple : quelques offres avec leur commune (jointure)
SELECT o.id, o.intitule, o.commune_code, o.code_postal, c.nom AS commune_nom, c.code_departement, c.code_region
FROM raw_france_travail_offres o
LEFT JOIN raw_geo_communes c ON c.code = o.commune_code
LIMIT 20;


-- -----------------------------------------------------------------------------
-- 4. LIEN OFFRES → GÉO (département via code postal)
-- -----------------------------------------------------------------------------
-- Si commune_code manque, on peut déduire le département du code postal (2 premiers chiffres en métropole).
-- Vérifier combien d'offres ont code_postal :

SELECT 'offres avec code_postal' AS lib, COUNT(*) AS nb
FROM raw_france_travail_offres WHERE code_postal IS NOT NULL AND code_postal != '';

-- Département = 2 premiers chiffres du code postal (approximation métropole)
SELECT SUBSTR(code_postal, 1, 2) AS dep_approx, COUNT(*) AS nb_offres
FROM raw_france_travail_offres
WHERE code_postal IS NOT NULL AND LENGTH(code_postal) >= 2
GROUP BY SUBSTR(code_postal, 1, 2)
ORDER BY nb_offres DESC
LIMIT 15;


-- -----------------------------------------------------------------------------
-- 5. LIEN SIRENE INTERNE : établissement → unité légale
-- -----------------------------------------------------------------------------
-- Chaque siret a un siren ; vérifier la cohérence.

SELECT 'etablissements dont le siren existe dans unite_legale' AS lib, COUNT(*) AS nb
FROM raw_sirene_etablissement e
WHERE EXISTS (SELECT 1 FROM raw_sirene_unite_legale u WHERE u.siren = e.siren);


-- -----------------------------------------------------------------------------
-- 6. LIEN OFFRES → SIRENE (par nom d’entreprise)
-- -----------------------------------------------------------------------------
-- Pas de SIRET dans l’API offres ; jointure possible par nom (fuzzy).
-- Colonne Sirene : souvent denominationUniteLegale ou denominationunitelegale (casse selon Parquet).

-- Exemple : offres dont le nom entreprise matche (au moins partiellement) une unité légale
-- Adapter le nom de colonne Sirene (denominationUniteLegale, denominationunitelegale, etc.)

/*
SELECT o.id, o.entreprise_nom, u.siren, u.denominationUniteLegale
FROM raw_france_travail_offres o
INNER JOIN raw_sirene_unite_legale u
  ON LOWER(TRIM(u.denominationUniteLegale)) = LOWER(TRIM(o.entreprise_nom))
LIMIT 20;
*/

-- Compter les noms d’entreprises distincts dans les offres
SELECT COUNT(DISTINCT entreprise_nom) AS nb_entreprises_distinctes
FROM raw_france_travail_offres;


-- -----------------------------------------------------------------------------
-- 7. SIRENE → GÉO (établissement a un code commune)
-- -----------------------------------------------------------------------------
-- Colonne établissement : souvent codeCommuneEtablissement ou codecommuneetablissement.

/*
SELECT e.siret, e.siren, e.codeCommuneEtablissement, c.nom AS commune_nom
FROM raw_sirene_etablissement e
LEFT JOIN raw_geo_communes c ON c.code = e.codeCommuneEtablissement
LIMIT 20;
*/


-- -----------------------------------------------------------------------------
-- 8. VUE D’ENSEMBLE POUR SCHÉMA EN ÉTOILE
-- -----------------------------------------------------------------------------
-- Fact = offres (une ligne = une offre).
-- Dimensions possibles :
--   - dim_geo (région, département, commune) → clé : commune_code ou (code_region, code_departement, code_commune)
--   - dim_entreprise (Sirene) → clé : siren ou siret (si on l’a côté offres)
--   - dim_contrat (type contrat)
--   - dim_rome (métier)
--   - dim_temps (date création)

-- Exemple : fact + geo (pour préparer la dim_geo)
SELECT
  o.id AS offre_id,
  o.intitule,
  o.rome_code,
  o.type_contrat,
  o.date_creation,
  o.commune_code,
  o.code_postal,
  o.entreprise_nom,
  o.salaire_libelle,
  o.code_naf,
  c.code AS commune_code_geo,
  c.nom AS commune_nom,
  c.code_departement,
  c.code_region,
  d.nom AS departement_nom,
  r.nom AS region_nom
FROM raw_france_travail_offres o
LEFT JOIN raw_geo_communes c ON c.code = o.commune_code
LEFT JOIN raw_geo_departements d ON d.code = c.code_departement
LEFT JOIN raw_geo_regions r ON r.code = c.code_region
LIMIT 50;


-- -----------------------------------------------------------------------------
-- 9. VALEURS DISTINCTES UTILES POUR DIMENSIONS
-- -----------------------------------------------------------------------------

SELECT DISTINCT type_contrat FROM raw_france_travail_offres ORDER BY 1;
SELECT DISTINCT rome_code, rome_libelle FROM raw_france_travail_offres ORDER BY 1 LIMIT 30;
