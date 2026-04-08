-- mart: fact_offres (SQLite)
-- objectif: table de fait simple, exploitable directement en BI.

DROP VIEW IF EXISTS fact_offres;

CREATE VIEW fact_offres AS
SELECT
    i.offre_id,
    i.date_creation,
    i.date_creation_day,
    i.intitule,
    i.rome_code,
    i.rome_libelle,
    i.type_contrat,
    i.code_postal,
    i.commune_code,
    c.nom AS commune_nom,
    c.code_departement AS departement_code,
    d.nom AS departement_nom,
    c.code_region AS region_code,
    r.nom AS region_nom,
    i.entreprise_nom,
    i.siren,
    i.denomination_sirene,
    i.code_naf_offre,
    i.code_naf_sirene,
    i.match_naf,
    i.has_salaire_info,
    i.salaire_libelle,
    CASE WHEN i.siren IS NOT NULL AND TRIM(i.siren) <> '' THEN 1 ELSE 0 END AS has_siren_match
FROM int_offres_entreprises i
LEFT JOIN raw_geo_communes c
    ON c.code = i.commune_code
LEFT JOIN raw_geo_departements d
    ON d.code = c.code_departement
LEFT JOIN raw_geo_regions r
    ON r.code = c.code_region;
