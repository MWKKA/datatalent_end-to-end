{{ config(materialized='table') }}

SELECT 'job_title' AS colonne, COUNT(*) AS nb_total, COUNTIF(job_title IS NULL) AS nb_null, ROUND(100 * COUNTIF(job_title IS NULL) / COUNT(*), 2) AS pct_null
FROM {{ ref('mart_offres_clean') }}
UNION ALL
SELECT 'company_name_std', COUNT(*), COUNTIF(company_name_std IS NULL), ROUND(100 * COUNTIF(company_name_std IS NULL) / COUNT(*), 2)
FROM {{ ref('mart_offres_clean') }}
UNION ALL
SELECT 'city_std', COUNT(*), COUNTIF(city_std IS NULL), ROUND(100 * COUNTIF(city_std IS NULL) / COUNT(*), 2)
FROM {{ ref('mart_offres_clean') }}
