{{ config(materialized='table') }}

SELECT
  company_name_std,
  ANY_VALUE(company_name_display) AS company_name_display,
  ANY_VALUE(siren) AS siren,
  COUNT(*) AS nb_offres
FROM {{ ref('mart_offres_clean') }}
WHERE company_name_std IS NOT NULL
GROUP BY company_name_std
ORDER BY nb_offres DESC
