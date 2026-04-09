{{ config(materialized='table') }}

SELECT *
FROM {{ ref('mart_offres_clean') }}
WHERE is_data_engineer = TRUE
