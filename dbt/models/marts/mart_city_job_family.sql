{{ config(materialized='table') }}

SELECT
  city_std AS ville,
  job_family,
  COUNT(*) AS nb_offres,
  ROUND(AVG(salary_avg), 2) AS salaire_moyen
FROM {{ ref('mart_offres_clean') }}
WHERE city_std IS NOT NULL
GROUP BY city_std, job_family
ORDER BY nb_offres DESC
