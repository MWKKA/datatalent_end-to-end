{{ config(materialized='table', alias='adzuna_jobs_clean') }}

/*
  Une ligne par offre Adzuna dans raw_offres_team — alimente int_adzuna_company_metrics.
  Champs salaires / created : chemins JSON usuels de l’API Adzuna (ajuster si la réponse change).
*/

SELECT
  NULLIF(
    LOWER(REGEXP_REPLACE(TRIM(COALESCE(o.company_name, '')), r'[^a-zA-Z0-9]', '')),
    ''
  ) AS company_name_clean,
  TRIM(o.company_name) AS company_name,
  SAFE_CAST(JSON_VALUE(o.payload_json, '$.salary_min') AS FLOAT64) AS salary_min,
  SAFE_CAST(JSON_VALUE(o.payload_json, '$.salary_max') AS FLOAT64) AS salary_max,
  COALESCE(
    SAFE_CAST(JSON_VALUE(o.payload_json, '$.created') AS TIMESTAMP),
    SAFE_CAST(JSON_VALUE(o.payload_json, '$.created_at') AS TIMESTAMP),
    TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(o.payload_json, '$.created') AS INT64))
  ) AS created_at,
  o.offer_id
FROM {{ source('raw', 'raw_offres_team') }} AS o
WHERE o.is_adzuna = 1
