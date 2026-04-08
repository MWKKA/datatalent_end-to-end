-- STAGING - France Travail uniquement
CREATE OR REPLACE TABLE `datatalent-simplon.staging.stg_offres_ft` AS
SELECT
  offer_id,
  source_offer_id,
  source_name,
  company_name,
  TRIM(REGEXP_REPLACE(LOWER(company_name), r'[^a-z0-9]', '')) AS company_name_clean,
  location_name,
  intitule AS title,
  rome_code,
  code_naf_offre,
  payload_json,
  ingested_at,
  last_seen_at
FROM `datatalent-simplon.raw.raw_offres_team`
WHERE source_name = 'france_travail'
  AND offer_id IS NOT NULL;

