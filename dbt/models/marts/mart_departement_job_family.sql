{{ config(materialized='table') }}

SELECT
  ANY_VALUE(departement_label) AS departement,
  job_family,
  COUNT(*) AS nb_offres,
  ROUND(AVG(salary_avg), 2) AS salaire_moyen
FROM {{ ref('mart_offres_clean') }}
WHERE departement_std IS NOT NULL
GROUP BY departement_std, job_family
ORDER BY nb_offres DESC
