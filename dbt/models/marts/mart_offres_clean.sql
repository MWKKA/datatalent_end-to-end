{{ config(materialized='table') }}

WITH source AS (
  SELECT
    offer_id,
    COALESCE(NULLIF(TRIM(title), ''), NULLIF(TRIM(intitule), '')) AS job_title,
    NULLIF(TRIM(title), '') AS title_raw,
    NULLIF(TRIM(intitule), '') AS intitule_raw,
    NULLIF(TRIM(company_name), '') AS company_name_raw,
    NULLIF(TRIM(company_name_clean), '') AS company_name_clean,
    NULLIF(TRIM(entreprise_nom), '') AS entreprise_nom,
    NULLIF(TRIM(sirene_name), '') AS sirene_name,
    NULLIF(TRIM(adzuna_company_name), '') AS adzuna_company_name,
    COALESCE(
      NULLIF(TRIM(company_name), ''),
      NULLIF(TRIM(entreprise_nom), ''),
      NULLIF(TRIM(sirene_name), ''),
      NULLIF(TRIM(adzuna_company_name), '')
    ) AS company_name_display,
    COALESCE(
      NULLIF(TRIM(company_name_clean), ''),
      LOWER(REGEXP_REPLACE(
        COALESCE(
          NULLIF(TRIM(company_name), ''),
          NULLIF(TRIM(entreprise_nom), ''),
          NULLIF(TRIM(sirene_name), ''),
          NULLIF(TRIM(adzuna_company_name), '')
        ),
        r'[^a-zA-Z0-9]',
        ''
      ))
    ) AS company_name_std,
    SAFE_CAST(sirene_matched AS INT64) AS sirene_matched,
    NULLIF(TRIM(match_method), '') AS match_method,
    SAFE_CAST(match_score AS INT64) AS match_score,
    SAFE_CAST(naf_match AS INT64) AS naf_match,
    NULLIF(TRIM(code_naf_offre), '') AS code_naf_offre,
    NULLIF(TRIM(code_naf_sirene), '') AS code_naf_sirene,
    SAFE_CAST(siren AS INT64) AS siren,
    SAFE_CAST(siret AS INT64) AS siret,
    COALESCE(NULLIF(TRIM(location_name), ''), NULLIF(TRIM(lieu_commune), '')) AS location_raw,
    NULLIF(TRIM(location_name), '') AS location_name_raw,
    NULLIF(TRIM(lieu_libelle), '') AS lieu_libelle,
    NULLIF(TRIM(lieu_commune), '') AS lieu_commune,
    NULLIF(TRIM(lieu_code_postal), '') AS postal_code,
    SAFE_CAST(lieu_latitude AS FLOAT64) AS latitude,
    SAFE_CAST(lieu_longitude AS FLOAT64) AS longitude,
    NULLIF(TRIM(type_contrat), '') AS type_contrat,
    NULLIF(TRIM(type_contrat_libelle), '') AS type_contrat_libelle,
    NULLIF(TRIM(nature_contrat), '') AS nature_contrat,
    SAFE_CAST(alternance AS BOOL) AS alternance_source,
    NULLIF(TRIM(experience_exige), '') AS experience_exige,
    NULLIF(TRIM(experience_libelle), '') AS experience_libelle,
    NULLIF(TRIM(duree_travail_libelle), '') AS duree_travail_libelle,
    SAFE_CAST(nombre_postes AS INT64) AS nb_positions,
    NULLIF(TRIM(salaire_libelle), '') AS salaire_libelle,
    SAFE_CAST(adzuna_avg_salary_min AS FLOAT64) AS salary_avg_min,
    SAFE_CAST(adzuna_avg_salary_max AS FLOAT64) AS salary_avg_max,
    SAFE_CAST(adzuna_min_salary_min AS FLOAT64) AS salary_min_raw,
    SAFE_CAST(adzuna_max_salary_max AS FLOAT64) AS salary_max_raw,
    NULLIF(TRIM(rome_code), '') AS rome_code,
    NULLIF(TRIM(rome_libelle), '') AS rome_libelle,
    NULLIF(TRIM(appellation_libelle), '') AS appellation_libelle,
    COALESCE(
      NULLIF(TRIM(code_naf), ''),
      NULLIF(TRIM(code_naf_sirene), ''),
      NULLIF(TRIM(code_naf_offre), '')
    ) AS code_naf_std,
    NULLIF(TRIM(secteur_activite), '') AS secteur_activite,
    NULLIF(TRIM(secteur_activite_libelle), '') AS secteur_activite_libelle,
    NULLIF(TRIM(ft_description), '') AS ft_description,
    NULLIF(TRIM(entreprise_description), '') AS entreprise_description,
    enriched_at,
    last_seen_at,
    date_creation,
    update_date AS date_actualisation,
    adzuna_latest_created_at,
    valid_from,
    valid_to
  FROM {{ ref('int_offres_all_sources_current') }}
),
location_std AS (
  SELECT
    *,
    CASE
      WHEN LOWER(COALESCE(location_raw, '')) = 'france' THEN NULL
      WHEN REGEXP_CONTAINS(COALESCE(location_raw, ''), r'^\d{2,3}\s*-\s*') THEN TRIM(REGEXP_REPLACE(COALESCE(location_raw, ''), r'^\d{2,3}\s*-\s*', ''))
      WHEN REGEXP_CONTAINS(COALESCE(location_raw, ''), r',') THEN TRIM(SPLIT(COALESCE(location_raw, ''), ',')[OFFSET(0)])
      ELSE TRIM(location_raw)
    END AS city_std
  FROM source
),
enriched AS (
  SELECT
    *,
    CASE
      WHEN salary_avg_min IS NOT NULL AND salary_avg_max IS NOT NULL THEN ROUND((salary_avg_min + salary_avg_max) / 2, 2)
      WHEN salary_avg_min IS NOT NULL THEN salary_avg_min
      WHEN salary_avg_max IS NOT NULL THEN salary_avg_max
      ELSE NULL
    END AS salary_avg,
    CASE
      WHEN salary_avg_min IS NOT NULL AND salary_avg_max IS NOT NULL THEN ROUND(salary_avg_max - salary_avg_min, 2)
      ELSE NULL
    END AS salary_range,
    CASE
      WHEN COALESCE(type_contrat_libelle, type_contrat, nature_contrat) IS NULL THEN 'Non renseigne'
      WHEN LOWER(COALESCE(type_contrat_libelle, type_contrat, nature_contrat, '')) LIKE '%cdi%' THEN 'CDI'
      WHEN LOWER(COALESCE(type_contrat_libelle, type_contrat, nature_contrat, '')) LIKE '%cdd%' THEN 'CDD'
      WHEN LOWER(COALESCE(type_contrat_libelle, type_contrat, nature_contrat, '')) LIKE '%altern%' THEN 'Alternance'
      WHEN LOWER(COALESCE(type_contrat_libelle, type_contrat, nature_contrat, '')) LIKE '%stage%' THEN 'Stage'
      ELSE COALESCE(type_contrat_libelle, type_contrat, nature_contrat)
    END AS contract_group,
    CASE
      WHEN experience_libelle IS NULL THEN 'Non renseigne'
      WHEN LOWER(COALESCE(experience_libelle, '')) LIKE '%junior%' THEN 'Junior'
      WHEN LOWER(COALESCE(experience_libelle, '')) LIKE '%senior%' THEN 'Senior'
      WHEN LOWER(COALESCE(experience_libelle, '')) LIKE '%debut%' THEN 'Debutant'
      WHEN LOWER(COALESCE(experience_libelle, '')) LIKE '%confirm%' THEN 'Confirme'
      ELSE experience_libelle
    END AS experience_group,
    CASE
      WHEN job_title IS NULL THEN 'Autre'
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'\bdata engineer\b') THEN 'Data Engineer'
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'ing[eé]nieur data') THEN 'Data Engineer'
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'\bdata analyst\b') THEN 'Data Analyst'
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'\bdata scientist\b') THEN 'Data Scientist'
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'\bml engineer\b|\bmachine learning engineer\b') THEN 'ML Engineer'
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'\bbi\b|\bbusiness intelligence\b') THEN 'BI / Analytics'
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'\bdata\b') THEN 'Autre metier data'
      ELSE 'Autre'
    END AS job_family,
    CASE
      WHEN job_title IS NULL THEN FALSE
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'\bdata engineer\b|ing[eé]nieur data|\bdatabricks\b|\betl\b|big data|cloud data') THEN TRUE
      ELSE FALSE
    END AS is_data_engineer,
    CASE
      WHEN alternance_source = TRUE THEN TRUE
      WHEN REGEXP_CONTAINS(LOWER(COALESCE(job_title, '')), r'\balternance\b|\bapprentissage\b|\bapprenti\b') THEN TRUE
      ELSE FALSE
    END AS alternance,
    CASE WHEN job_title IS NOT NULL THEN TRUE ELSE FALSE END AS has_job_title,
    CASE WHEN company_name_display IS NOT NULL THEN TRUE ELSE FALSE END AS has_company,
    CASE WHEN city_std IS NOT NULL THEN TRUE ELSE FALSE END AS has_city,
    CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN TRUE ELSE FALSE END AS has_coordinates,
    CASE WHEN salary_avg_min IS NOT NULL OR salary_avg_max IS NOT NULL THEN TRUE ELSE FALSE END AS has_salary,
    CASE WHEN sirene_matched = 1 THEN TRUE ELSE FALSE END AS has_sirene_match
  FROM location_std
)
SELECT * FROM enriched
