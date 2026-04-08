-- intermediate offres <-> entreprises (SQLite)
-- objectif: consolider les infos offre + matching Sirene.

DROP VIEW IF EXISTS int_offres_entreprises;

CREATE VIEW int_offres_entreprises AS
SELECT
    o.offre_id,
    o.intitule,
    o.rome_code,
    o.rome_libelle,
    o.type_contrat,
    o.date_creation,
    o.date_creation_day,
    o.lieu_libelle,
    o.code_postal,
    o.commune_code,
    o.entreprise_nom,
    o.entreprise_nom_norm,
    o.salaire_libelle,
    o.has_salaire_info,
    o.code_naf AS code_naf_offre,
    o.secteur_activite_libelle,
    m.siren,
    m.denomination_sirene,
    m.match_naf,
    u.activitePrincipaleUniteLegale AS code_naf_sirene
FROM stg_offres o
LEFT JOIN offres_avec_siren m
    ON m.offre_id = o.offre_id
LEFT JOIN raw_sirene_unite_legale u
    ON u.siren = m.siren;
