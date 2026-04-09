{{ config(materialized='table') }}

SELECT
  company_name_clean,
  ANY_VALUE(company_name) AS company_name,
  COUNT(*) AS adzuna_job_count,
  AVG(salary_min) AS adzuna_avg_salary_min,
  AVG(salary_max) AS adzuna_avg_salary_max,
  MIN(salary_min) AS adzuna_min_salary_min,
  MAX(salary_max) AS adzuna_max_salary_max,
  MAX(created_at) AS adzuna_latest_created_at
FROM {{ source('staging', 'adzuna_jobs_clean') }}
WHERE company_name_clean IS NOT NULL
GROUP BY company_name_clean
