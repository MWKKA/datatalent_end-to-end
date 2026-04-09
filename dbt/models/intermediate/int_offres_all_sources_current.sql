{{ config(materialized='view') }}

SELECT
  *,
  dbt_valid_from AS valid_from,
  dbt_valid_to AS valid_to,
  dbt_valid_to IS NULL AS is_current
FROM {{ ref('snp_offres_all_sources_scd2') }}
WHERE dbt_valid_to IS NULL
