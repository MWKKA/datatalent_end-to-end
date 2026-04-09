{{ config(materialized='table') }}

SELECT
  company_name_std,
  ANY_VALUE(company_name_display) AS company_name_display,
  job_family,
  COUNT(*) AS nb_offres,
  ROUND(AVG(salary_avg), 2) AS salaire_moyen
FROM {{ ref('mart_offres_clean') }}
WHERE company_name_std IS NOT NULL
GROUP BY company_name_std, job_family
ORDER BY nb_offres DESC
