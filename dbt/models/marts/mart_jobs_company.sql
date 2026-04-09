{{ config(materialized='table') }}

SELECT
  company_name_std,
  ANY_VALUE(company_name_display) AS company_name_display,
  ANY_VALUE(siren) AS siren,
  COUNT(*) AS nb_offres,
  COUNT(DISTINCT city_std) AS nb_villes,
  COUNTIF(has_salary) AS nb_offres_avec_salaire,
  ROUND(AVG(salary_avg), 2) AS salaire_moyen,
  APPROX_QUANTILES(salary_avg, 100)[OFFSET(50)] AS salaire_median,
  MIN(salary_avg) AS salaire_min,
  MAX(salary_avg) AS salaire_max
FROM {{ ref('mart_offres_clean') }}
WHERE company_name_std IS NOT NULL
GROUP BY company_name_std
ORDER BY nb_offres DESC
