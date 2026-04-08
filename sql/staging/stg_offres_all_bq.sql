-- STAGING - Offres unifiees multi-sources
CREATE OR REPLACE TABLE `datatalent-simplon.staging.stg_offres_all` AS
SELECT
  offer_id,
  source_offer_id,
  source_name,
  TRUE AS is_france_travail,
  FALSE AS is_adzuna,
  company_name,
  company_name_clean,
  location_name,
  title,
  rome_code,
  code_naf_offre,
  payload_json,
  ingested_at,
  last_seen_at
FROM `datatalent-simplon.staging.stg_offres_ft`

UNION ALL

SELECT
  offer_id,
  source_offer_id,
  source_name,
  FALSE AS is_france_travail,
  TRUE AS is_adzuna,
  company_name,
  company_name_clean,
  location_name,
  title,
  rome_code,
  code_naf_offre,
  payload_json,
  ingested_at,
  last_seen_at
FROM `datatalent-simplon.staging.stg_offres_adzuna`;

