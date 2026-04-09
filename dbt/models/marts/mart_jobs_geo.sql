{{ config(materialized='table') }}

SELECT
  ANY_VALUE(departement_label) AS departement,
  ANY_VALUE(postal_code) AS code_postal_exemple,
  ANY_VALUE(latitude) AS latitude,
  ANY_VALUE(longitude) AS longitude,
  COUNT(*) AS nb_offres,
  COUNT(DISTINCT company_name_std) AS nb_entreprises,
  COUNTIF(has_salary) AS nb_offres_avec_salaire,
  ROUND(AVG(salary_avg), 2) AS salaire_moyen,
  APPROX_QUANTILES(salary_avg, 100)[OFFSET(50)] AS salaire_median,
  MIN(salary_avg) AS salaire_min,
  MAX(salary_avg) AS salaire_max
FROM {{ ref('mart_offres_clean') }}
WHERE departement_std IS NOT NULL
GROUP BY departement_std
ORDER BY nb_offres DESC
