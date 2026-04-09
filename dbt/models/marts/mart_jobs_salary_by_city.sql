{{ config(materialized='table') }}

SELECT
  city_std AS ville,
  COUNT(*) AS nb_offres,
  COUNTIF(has_salary) AS nb_offres_avec_salaire,
  ROUND(AVG(salary_avg), 2) AS salaire_moyen,
  APPROX_QUANTILES(salary_avg, 100)[OFFSET(50)] AS salaire_median,
  MIN(salary_avg) AS salaire_min,
  MAX(salary_avg) AS salaire_max
FROM {{ ref('mart_offres_clean') }}
WHERE city_std IS NOT NULL
GROUP BY city_std
HAVING COUNT(*) >= 3
ORDER BY salaire_moyen DESC
