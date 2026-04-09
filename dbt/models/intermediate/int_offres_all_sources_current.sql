{{ config(materialized='view') }}

SELECT
  * EXCEPT (dbt_valid_from, dbt_valid_to, dbt_scd_id),
  dbt_valid_from AS valid_from,
  dbt_valid_to AS valid_to,
  dbt_valid_to IS NULL AS is_current,
  CAST(NULL AS TIMESTAMP) AS dl_insert_date,
  CAST(NULL AS TIMESTAMP) AS dl_update_date
FROM {{ ref('snp_offres_all_sources_scd2') }}
WHERE dbt_valid_to IS NULL
