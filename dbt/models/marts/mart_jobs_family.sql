{{ config(materialized='table') }}

SELECT
  job_family,
  COUNT(*) AS nb_offres,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_offres,
  COUNT(DISTINCT company_name_std) AS nb_entreprises,
  COUNT(DISTINCT departement_std) AS nb_departements,
  ROUND(AVG(salary_avg), 2) AS salaire_moyen,
  APPROX_QUANTILES(salary_avg, 100)[OFFSET(50)] AS salaire_median
FROM {{ ref('mart_offres_clean') }}
GROUP BY job_family
ORDER BY nb_offres DESC
