{{ config(materialized='table') }}

SELECT
  COUNT(*) AS nb_offres_total,
  COUNT(DISTINCT company_name_std) AS nb_entreprises_distinctes,
  COUNT(DISTINCT city_std) AS nb_villes_distinctes,
  COUNTIF(has_salary) AS nb_offres_avec_salaire,
  ROUND(100 * COUNTIF(has_salary) / COUNT(*), 2) AS pct_offres_avec_salaire,
  COUNTIF(has_sirene_match) AS nb_offres_matchees_sirene,
  ROUND(100 * COUNTIF(has_sirene_match) / COUNT(*), 2) AS pct_match_sirene,
  COUNTIF(alternance = TRUE) AS nb_offres_alternance,
  ROUND(AVG(nb_positions), 2) AS nb_postes_moyen,
  ROUND(AVG(salary_avg), 2) AS salaire_moyen,
  APPROX_QUANTILES(salary_avg, 100)[OFFSET(50)] AS salaire_median,
  MIN(salary_avg) AS salaire_min,
  MAX(salary_avg) AS salaire_max
FROM {{ ref('mart_offres_data_engineer') }}
