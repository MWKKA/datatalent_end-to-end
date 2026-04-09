{{ config(materialized='table') }}

SELECT
  'job_title' AS colonne,
  COUNT(*) AS nb_total,
  COUNTIF(job_title IS NULL) AS nb_null,
  ROUND(100 * COUNTIF(job_title IS NULL) / COUNT(*), 2) AS pct_null
FROM {{ ref('mart_offres_clean') }}

UNION ALL
SELECT 'company_name_display', COUNT(*), COUNTIF(company_name_display IS NULL), ROUND(100 * COUNTIF(company_name_display IS NULL) / COUNT(*), 2)
FROM {{ ref('mart_offres_clean') }}

UNION ALL
SELECT 'company_name_std', COUNT(*), COUNTIF(company_name_std IS NULL), ROUND(100 * COUNTIF(company_name_std IS NULL) / COUNT(*), 2)
FROM {{ ref('mart_offres_clean') }}

UNION ALL
SELECT 'city_std', COUNT(*), COUNTIF(city_std IS NULL), ROUND(100 * COUNTIF(city_std IS NULL) / COUNT(*), 2)
FROM {{ ref('mart_offres_clean') }}

UNION ALL
SELECT 'departement_std', COUNT(*), COUNTIF(departement_std IS NULL), ROUND(100 * COUNTIF(departement_std IS NULL) / COUNT(*), 2)
FROM {{ ref('mart_offres_clean') }}

UNION ALL
SELECT 'salary_avg', COUNT(*), COUNTIF(salary_avg IS NULL), ROUND(100 * COUNTIF(salary_avg IS NULL) / COUNT(*), 2)
FROM {{ ref('mart_offres_clean') }}

UNION ALL
SELECT 'contract_group', COUNT(*), COUNTIF(contract_group IS NULL), ROUND(100 * COUNTIF(contract_group IS NULL) / COUNT(*), 2)
FROM {{ ref('mart_offres_clean') }}

UNION ALL
SELECT 'experience_group', COUNT(*), COUNTIF(experience_group IS NULL), ROUND(100 * COUNTIF(experience_group IS NULL) / COUNT(*), 2)
FROM {{ ref('mart_offres_clean') }}

UNION ALL
SELECT 'job_family', COUNT(*), COUNTIF(job_family IS NULL), ROUND(100 * COUNTIF(job_family IS NULL) / COUNT(*), 2)
FROM {{ ref('mart_offres_clean') }}

UNION ALL
SELECT 'siren', COUNT(*), COUNTIF(siren IS NULL), ROUND(100 * COUNTIF(siren IS NULL) / COUNT(*), 2)
FROM {{ ref('mart_offres_clean') }}

UNION ALL
SELECT 'latitude', COUNT(*), COUNTIF(latitude IS NULL), ROUND(100 * COUNTIF(latitude IS NULL) / COUNT(*), 2)
FROM {{ ref('mart_offres_clean') }}

UNION ALL
SELECT 'longitude', COUNT(*), COUNTIF(longitude IS NULL), ROUND(100 * COUNTIF(longitude IS NULL) / COUNT(*), 2)
FROM {{ ref('mart_offres_clean') }}
