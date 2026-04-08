-- STAGING - Harmonisation multi-sources (France Travail + Adzuna)
-- Sources equipe reutilisees:
-- - staging.offres_staging
-- - staging.adzuna_jobs_clean

CREATE OR REPLACE TABLE `datatalent-simplon.staging.stg_offres_all_sources` AS
WITH france_travail AS (
  SELECT
    id_offre AS offer_id,
    'france_travail' AS source_name,
    TRUE AS is_france_travail,
    FALSE AS is_adzuna,
    entreprise_nom AS company_name,
    TRIM(REGEXP_REPLACE(LOWER(entreprise_nom), r'[^a-z0-9]', '')) AS company_name_clean,
    intitule AS title,
    description,
    rome_code,
    code_naf AS code_naf_offre,
    lieu_libelle AS location_name,
    lieu_commune AS commune_code,
    lieu_code_postal AS code_postal,
    date_creation AS created_at,
    ingestion_timestamp,
    source_file
  FROM `datatalent-simplon.staging.offres_staging`
  WHERE id_offre IS NOT NULL
),
adzuna AS (
  SELECT
    CAST(job_id AS STRING) AS offer_id,
    'adzuna' AS source_name,
    FALSE AS is_france_travail,
    TRUE AS is_adzuna,
    company_name,
    TRIM(REGEXP_REPLACE(LOWER(company_name), r'[^a-z0-9]', '')) AS company_name_clean,
    title,
    description,
    CAST(NULL AS STRING) AS rome_code,
    CAST(NULL AS STRING) AS code_naf_offre,
    location_display_name AS location_name,
    CAST(NULL AS STRING) AS commune_code,
    CAST(NULL AS STRING) AS code_postal,
    created_at,
    ingestion_timestamp,
    CAST(NULL AS STRING) AS source_file
  FROM `datatalent-simplon.staging.adzuna_jobs_clean`
  WHERE job_id IS NOT NULL
)
SELECT *
FROM france_travail
WHERE company_name IS NOT NULL
  AND title IS NOT NULL
  AND (
    LOWER(title) LIKE '%data engineer%'
    OR LOWER(title) LIKE '%ingenieur data%'
    OR LOWER(title) LIKE '%ingénieur data%'
  )

UNION ALL

SELECT *
FROM adzuna
WHERE company_name IS NOT NULL
  AND title IS NOT NULL
  AND (
    LOWER(title) LIKE '%data engineer%'
    OR LOWER(title) LIKE '%ingenieur data%'
    OR LOWER(title) LIKE '%ingénieur data%'
  );

