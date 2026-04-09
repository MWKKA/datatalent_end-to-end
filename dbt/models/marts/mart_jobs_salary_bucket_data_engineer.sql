{{ config(materialized='table') }}

SELECT
  salary_bucket,
  COUNT(*) AS nb_offres,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_offres
FROM {{ ref('mart_offres_data_engineer') }}
GROUP BY salary_bucket
ORDER BY
  CASE salary_bucket
    WHEN '<35k' THEN 1
    WHEN '35k-45k' THEN 2
    WHEN '45k-55k' THEN 3
    WHEN '55k-65k' THEN 4
    WHEN '65k+' THEN 5
    ELSE 6
  END
