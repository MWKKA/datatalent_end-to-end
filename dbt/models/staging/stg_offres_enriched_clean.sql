{{ config(materialized='table', alias='offres_enriched_clean') }}

/*
  Pont raw → staging : aligné sur le job Python (raw.offres_enriched_team + raw.raw_offres_team).

  Colonnes volontairement sans chevauchement avec offres_staging (intitule, lieu_*, etc. viennent du JOIN FT).
  title : réservé aux offres Adzuna (JSON) ; le mart fait COALESCE(intitule, title).
*/

SELECT
  e.offer_id,
  COALESCE(o.company_name, e.company_name) AS company_name,
  NULLIF(
    LOWER(REGEXP_REPLACE(TRIM(COALESCE(o.company_name, e.company_name, '')), r'[^a-zA-Z0-9]', '')),
    ''
  ) AS company_name_clean,
  CASE
    WHEN o.is_adzuna = 1 THEN JSON_VALUE(o.payload_json, '$.title')
    ELSE CAST(NULL AS STRING)
  END AS title,
  o.location_name AS location_name,
  CAST(e.sirene_matched AS INT64) AS sirene_matched,
  e.match_method,
  CAST(e.match_score AS INT64) AS match_score,
  CAST(e.naf_match AS INT64) AS naf_match,
  e.code_naf_offre,
  e.code_naf_sirene,
  e.sirene_name,
  e.siren,
  e.siret,
  e.enriched_at,
  e.last_seen_at
FROM {{ source('raw', 'offres_enriched_team') }} AS e
INNER JOIN {{ source('raw', 'raw_offres_team') }} AS o
  ON e.offer_id = o.offer_id
