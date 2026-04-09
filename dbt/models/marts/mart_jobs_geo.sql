{{ config(materialized='table') }}

SELECT
  city_std AS ville,
  ANY_VALUE(postal_code) AS code_postal,
  ANY_VALUE(latitude) AS latitude,
  ANY_VALUE(longitude) AS longitude,
  COUNT(*) AS nb_offres,
  COUNT(DISTINCT company_name_std) AS nb_entreprises
FROM {{ ref('mart_offres_clean') }}
WHERE city_std IS NOT NULL
GROUP BY city_std
ORDER BY nb_offres DESC
