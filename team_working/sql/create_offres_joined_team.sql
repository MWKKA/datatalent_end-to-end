-- Table locale de travail: jointure offres raw + enrichissement Sirene.
-- Objectif: avoir une vue exploitable rapidement dans DBeaver.

DROP TABLE IF EXISTS offres_joined_team;

CREATE TABLE offres_joined_team AS
SELECT
    o.offer_id,
    o.source_offer_id,
    o.source_name,
    o.is_france_travail,
    o.is_adzuna,
    o.company_name AS company_name_offer,
    LOWER(TRIM(REPLACE(REPLACE(REPLACE(COALESCE(o.company_name, ''), '-', ' '), '.', ' '), '''', '')))
        AS company_name_offer_clean,
    o.location_name,
    o.intitule AS title,
    o.rome_code,
    o.code_naf_offre,
    json_extract(o.payload_json, '$.lieuTravail.commune') AS commune_code_offer,
    json_extract(o.payload_json, '$.lieuTravail.codePostal') AS code_postal_offer,
    e.sirene_matched,
    e.match_method,
    e.match_score,
    e.naf_match,
    e.code_naf_sirene,
    e.sirene_name,
    e.siren,
    e.siret,
    e.enriched_at,
    e.last_seen_at AS enriched_last_seen_at,
    o.last_seen_at AS offer_last_seen_at
FROM raw_offres_team o
LEFT JOIN offres_enriched_team e
    ON e.offer_id = o.offer_id;

CREATE INDEX IF NOT EXISTS idx_offres_joined_team_offer_id
    ON offres_joined_team (offer_id);

CREATE INDEX IF NOT EXISTS idx_offres_joined_team_source
    ON offres_joined_team (source_name);

CREATE INDEX IF NOT EXISTS idx_offres_joined_team_sirene_matched
    ON offres_joined_team (sirene_matched);
