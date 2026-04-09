{{ config(materialized='table') }}

SELECT
  CASE
    WHEN salary_avg IS NULL THEN 'Non renseigne'
    WHEN salary_avg < 35000 THEN '<35k'
    WHEN salary_avg < 45000 THEN '35k-45k'
    WHEN salary_avg < 55000 THEN '45k-55k'
    WHEN salary_avg < 65000 THEN '55k-65k'
    ELSE '65k+'
  END AS salary_bucket,
  COUNT(*) AS nb_offres,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_offres
FROM {{ ref('mart_offres_clean') }}
GROUP BY 1
