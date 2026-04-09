{{ config(materialized='table') }}

SELECT
  contract_group,
  COUNT(*) AS nb_offres,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_offres,
  ROUND(AVG(salary_avg), 2) AS salaire_moyen,
  APPROX_QUANTILES(salary_avg, 100)[OFFSET(50)] AS salaire_median
FROM {{ ref('mart_offres_data_engineer') }}
GROUP BY contract_group
ORDER BY nb_offres DESC
