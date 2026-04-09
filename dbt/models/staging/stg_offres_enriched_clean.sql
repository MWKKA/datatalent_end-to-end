{{ config(materialized='table') }}

SELECT
  e.offer_id,
  o.intitule AS title,
  e.company_name,
  TRIM(REGEXP_REPLACE(LOWER(e.company_name), r'[^a-z0-9]', '')) AS company_name_clean,
  o.location_name,
  e.sirene_matched,
  e.match_method,
  e.match_score,
  e.naf_match,
  e.code_naf_offre,
  e.code_naf_sirene,
  e.sirene_name,
  e.siren,
  e.siret,
  e.enriched_at,
  e.last_seen_at
FROM {{ source('raw', 'offres_enriched_team') }} e
LEFT JOIN {{ source('raw', 'raw_offres_team') }} o
  ON o.offer_id = e.offer_id
WHERE e.offer_id IS NOT NULL
