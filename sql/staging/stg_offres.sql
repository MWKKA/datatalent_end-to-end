-- staging offres (SQLite)
-- objectif: normaliser les colonnes utiles pour les couches suivantes.

DROP VIEW IF EXISTS stg_offres;

CREATE VIEW stg_offres AS
SELECT
    TRIM(id) AS offre_id,
    TRIM(intitule) AS intitule,
    TRIM(rome_code) AS rome_code,
    TRIM(rome_libelle) AS rome_libelle,
    TRIM(type_contrat) AS type_contrat,
    -- date_creation est gardee en texte ISO (convertible par date())
    TRIM(date_creation) AS date_creation,
    SUBSTR(TRIM(date_creation), 1, 10) AS date_creation_day,
    TRIM(lieu_libelle) AS lieu_libelle,
    TRIM(code_postal) AS code_postal,
    TRIM(commune_code) AS commune_code,
    LOWER(TRIM(entreprise_nom)) AS entreprise_nom_norm,
    TRIM(entreprise_nom) AS entreprise_nom,
    TRIM(salaire_libelle) AS salaire_libelle,
    CASE
        WHEN salaire_libelle IS NULL OR TRIM(salaire_libelle) = '' THEN 0
        ELSE 1
    END AS has_salaire_info,
    TRIM(code_naf) AS code_naf,
    TRIM(secteur_activite_libelle) AS secteur_activite_libelle
FROM raw_france_travail_offres
WHERE id IS NOT NULL AND TRIM(id) <> '';
