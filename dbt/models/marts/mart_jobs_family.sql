{{ config(materialized='table') }}

SELECT
  job_family,
  COUNT(*) AS nb_offres,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_offres
FROM {{ ref('mart_offres_clean') }}
GROUP BY job_family
ORDER BY nb_offres DESC
