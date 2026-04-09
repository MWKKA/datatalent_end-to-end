{{ config(materialized='table') }}

WITH source AS (
  SELECT
    offer_id,
    COALESCE(
      NULLIF(TRIM(title), ''),
      NULLIF(TRIM(intitule), '')
    ) AS job_title,
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
      LOWER(
        REGEXP_REPLACE(
          COALESCE(
            NULLIF(TRIM(company_name), ''),
            NULLIF(TRIM(entreprise_nom), ''),
            NULLIF(TRIM(sirene_name), ''),
            NULLIF(TRIM(adzuna_company_name), '')
          ),
          r'[^a-zA-Z0-9]',
          ''
        )
      )
    ) AS company_name_std,
    SAFE_CAST(sirene_matched AS INT64) AS sirene_matched,
    NULLIF(TRIM(match_method), '') AS match_method,
    SAFE_CAST(match_score AS INT64) AS match_score,
    SAFE_CAST(naf_match AS INT64) AS naf_match,
    NULLIF(TRIM(code_naf_offre), '') AS code_naf_offre,
    NULLIF(TRIM(code_naf_sirene), '') AS code_naf_sirene,
    SAFE_CAST(siren AS INT64) AS siren,
    SAFE_CAST(siret AS INT64) AS siret,
    COALESCE(
      CASE
        WHEN REGEXP_CONTAINS(TRIM(COALESCE(lieu_libelle, '')), r'^\d{5}$')
          THEN COALESCE(
            NULLIF(TRIM(location_name), ''),
            NULLIF(TRIM(lieu_commune), '')
          )
        ELSE NULLIF(TRIM(lieu_libelle), '')
      END,
      NULLIF(TRIM(location_name), ''),
      NULLIF(TRIM(lieu_commune), '')
    ) AS location_raw,
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
    valid_to,
    dl_insert_date,
    dl_update_date
  FROM {{ ref('int_offres_all_sources_current') }}
),
location_with_cp AS (
  SELECT
    *,
    COALESCE(
      CASE
        WHEN LENGTH(REGEXP_REPLACE(TRIM(COALESCE(postal_code, '')), r'[^0-9]', '')) = 5
          THEN REGEXP_REPLACE(TRIM(COALESCE(postal_code, '')), r'[^0-9]', '')
        ELSE NULL
      END,
      CASE
        WHEN REGEXP_CONTAINS(TRIM(COALESCE(lieu_libelle, '')), r'^\d{5}$')
          THEN REGEXP_REPLACE(TRIM(COALESCE(lieu_libelle, '')), r'[^0-9]', '')
        ELSE NULL
      END,
      CASE
        WHEN LENGTH(REGEXP_REPLACE(TRIM(COALESCE(lieu_libelle, '')), r'[^0-9]', '')) >= 5
          THEN SUBSTR(REGEXP_REPLACE(TRIM(COALESCE(lieu_libelle, '')), r'[^0-9]', ''), 1, 5)
        ELSE NULL
      END,
      CASE
        WHEN REGEXP_CONTAINS(TRIM(COALESCE(location_name_raw, '')), r'^\d{5}$')
          THEN REGEXP_REPLACE(TRIM(COALESCE(location_name_raw, '')), r'[^0-9]', '')
        ELSE NULL
      END,
      CASE
        WHEN REGEXP_CONTAINS(TRIM(COALESCE(lieu_commune, '')), r'^\d{5}$')
          THEN REGEXP_REPLACE(TRIM(COALESCE(lieu_commune, '')), r'[^0-9]', '')
        ELSE NULL
      END,
      CASE
        WHEN REGEXP_CONTAINS(
          CONCAT(
            COALESCE(lieu_libelle, ''),
            ' ',
            COALESCE(lieu_commune, ''),
            ' ',
            COALESCE(location_name_raw, '')
          ),
          r'\d{5}'
        )
          THEN REGEXP_EXTRACT(
            CONCAT(
              COALESCE(lieu_libelle, ''),
              ' ',
              COALESCE(lieu_commune, ''),
              ' ',
              COALESCE(location_name_raw, '')
            ),
            r'(?:^|[^0-9])(\d{5})(?:[^0-9]|$)'
          )
        ELSE NULL
      END
    ) AS cp_norm
  FROM source
),
location_inner AS (
  SELECT
    * EXCEPT (cp_norm),
    CASE
      WHEN LOWER(COALESCE(location_raw, '')) = 'france' THEN NULL
      WHEN REGEXP_CONTAINS(COALESCE(location_raw, ''), r'^\d{5}$') THEN NULL
      WHEN REGEXP_CONTAINS(COALESCE(location_raw, ''), r'^\d{2,3}\s*-\s*') THEN
        TRIM(REGEXP_REPLACE(COALESCE(location_raw, ''), r'^\d{2,3}\s*-\s*', ''))
      WHEN REGEXP_CONTAINS(COALESCE(location_raw, ''), r',') THEN
        TRIM(SPLIT(COALESCE(location_raw, ''), ',')[OFFSET(0)])
      ELSE TRIM(location_raw)
    END AS city_std,
    CASE
      WHEN LOWER(COALESCE(location_raw, '')) = 'france' THEN 'France'
      WHEN REGEXP_CONTAINS(COALESCE(location_raw, ''), r',') THEN
        TRIM(SPLIT(COALESCE(location_raw, ''), ',')[SAFE_OFFSET(1)])
      ELSE NULL
    END AS location_secondary_std,
    CASE
      WHEN cp_norm IS NOT NULL THEN
        CASE
          WHEN cp_norm BETWEEN '97000' AND '99999' THEN SUBSTR(cp_norm, 1, 3)
          WHEN cp_norm BETWEEN '20000' AND '20199' THEN '2A'
          WHEN cp_norm BETWEEN '20200' AND '20689' THEN '2B'
          ELSE SUBSTR(cp_norm, 1, 2)
        END
      WHEN REGEXP_CONTAINS(COALESCE(location_raw, ''), r'^\d{2,3}\s*-\s*') THEN
        REGEXP_EXTRACT(COALESCE(location_raw, ''), r'^(\d{2,3})\s*-\s*')
      ELSE NULL
    END AS departement_from_cp
  FROM location_with_cp
),
location_std AS (
  SELECT
    * EXCEPT (departement_from_cp),
    COALESCE(
      departement_from_cp,
      {{ departement_depuis_texte('location_secondary_std') }},
      {{ departement_depuis_texte('city_std') }},
      {{ departement_depuis_texte('lieu_commune') }},
      {{ departement_depuis_texte('location_name_raw') }},
      {{ departement_depuis_texte('lieu_libelle') }},
      {{ departement_depuis_ville('city_std') }},
      {{ departement_depuis_ville('lieu_commune') }},
      {{ departement_depuis_ville('location_name_raw') }},
      {{ departement_depuis_ville('lieu_libelle') }}
    ) AS departement_std
  FROM location_inner
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
      WHEN (
        CASE
          WHEN salary_avg_min IS NOT NULL AND salary_avg_max IS NOT NULL THEN (salary_avg_min + salary_avg_max) / 2
          WHEN salary_avg_min IS NOT NULL THEN salary_avg_min
          WHEN salary_avg_max IS NOT NULL THEN salary_avg_max
          ELSE NULL
        END
      ) IS NULL THEN 'Non renseigné'
      WHEN (
        CASE
          WHEN salary_avg_min IS NOT NULL AND salary_avg_max IS NOT NULL THEN (salary_avg_min + salary_avg_max) / 2
          WHEN salary_avg_min IS NOT NULL THEN salary_avg_min
          WHEN salary_avg_max IS NOT NULL THEN salary_avg_max
        END
      ) < 35000 THEN '<35k'
      WHEN (
        CASE
          WHEN salary_avg_min IS NOT NULL AND salary_avg_max IS NOT NULL THEN (salary_avg_min + salary_avg_max) / 2
          WHEN salary_avg_min IS NOT NULL THEN salary_avg_min
          WHEN salary_avg_max IS NOT NULL THEN salary_avg_max
        END
      ) < 45000 THEN '35k-45k'
      WHEN (
        CASE
          WHEN salary_avg_min IS NOT NULL AND salary_avg_max IS NOT NULL THEN (salary_avg_min + salary_avg_max) / 2
          WHEN salary_avg_min IS NOT NULL THEN salary_avg_min
          WHEN salary_avg_max IS NOT NULL THEN salary_avg_max
        END
      ) < 55000 THEN '45k-55k'
      WHEN (
        CASE
          WHEN salary_avg_min IS NOT NULL AND salary_avg_max IS NOT NULL THEN (salary_avg_min + salary_avg_max) / 2
          WHEN salary_avg_min IS NOT NULL THEN salary_avg_min
          WHEN salary_avg_max IS NOT NULL THEN salary_avg_max
        END
      ) < 65000 THEN '55k-65k'
      ELSE '65k+'
    END AS salary_bucket,
    CASE
      WHEN REGEXP_CONTAINS(
        LOWER(CONCAT(
          COALESCE(type_contrat_libelle, ''), ' ',
          COALESCE(type_contrat, ''), ' ',
          COALESCE(nature_contrat, ''), ' ',
          COALESCE(duree_travail_libelle, ''), ' ',
          COALESCE(salaire_libelle, ''), ' ',
          SUBSTR(COALESCE(ft_description, ''), 1, 4000)
        )),
        r'\bcdi\b|dur[ée]e\s+ind[ée]termin[ée]e|contrat\s+[àa]\s+dur[ée]e\s+ind[ée]termin[ée]e'
      )
        OR LOWER(TRIM(COALESCE(type_contrat, ''))) IN ('e1', 'cdi')
        OR LOWER(TRIM(COALESCE(type_contrat_libelle, ''))) LIKE '%durée indéterminée%'
        OR LOWER(TRIM(COALESCE(type_contrat_libelle, ''))) LIKE '%duree indeterminee%'
      THEN 'CDI'
      WHEN REGEXP_CONTAINS(
        LOWER(CONCAT(
          COALESCE(type_contrat_libelle, ''), ' ',
          COALESCE(type_contrat, ''), ' ',
          COALESCE(nature_contrat, ''), ' ',
          COALESCE(duree_travail_libelle, ''), ' ',
          COALESCE(salaire_libelle, ''), ' ',
          SUBSTR(COALESCE(ft_description, ''), 1, 4000)
        )),
        r'\bcdd\b|dur[ée]e\s+d[ée]termin[ée]e|contrat\s+[àa]\s+dur[ée]e\s+d[ée]termin[ée]e'
      )
        OR LOWER(TRIM(COALESCE(type_contrat, ''))) IN ('e2', 'cdd')
        OR LOWER(TRIM(COALESCE(type_contrat_libelle, ''))) LIKE '%durée déterminée%'
        OR LOWER(TRIM(COALESCE(type_contrat_libelle, ''))) LIKE '%duree determinee%'
      THEN 'CDD'
      WHEN LOWER(COALESCE(type_contrat_libelle, type_contrat, nature_contrat, '')) LIKE '%altern%' THEN 'Alternance'
      WHEN LOWER(COALESCE(type_contrat_libelle, type_contrat, nature_contrat, '')) LIKE '%stage%' THEN 'Stage'
      WHEN COALESCE(type_contrat_libelle, type_contrat, nature_contrat) IS NULL THEN 'Non renseigné'
      ELSE COALESCE(type_contrat_libelle, type_contrat, nature_contrat)
    END AS contract_group,
    CASE
      WHEN LOWER(COALESCE(experience_libelle, '')) LIKE '%junior%' THEN 'Junior'
      WHEN LOWER(COALESCE(experience_libelle, '')) LIKE '%senior%' THEN 'Senior'
      WHEN LOWER(COALESCE(experience_libelle, '')) LIKE '%début%' THEN 'Débutant'
      WHEN LOWER(COALESCE(experience_libelle, '')) LIKE '%debut%' THEN 'Débutant'
      WHEN LOWER(COALESCE(experience_libelle, '')) LIKE '%confirm%' THEN 'Confirmé'
      WHEN experience_libelle IS NULL THEN 'Non renseigné'
      ELSE experience_libelle
    END AS experience_group,
    CASE
      WHEN job_title IS NULL THEN 'Autre'
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'\bdata engineer\b') THEN 'Data Engineer'
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'ing[eé]nieur data') THEN 'Data Engineer'
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'\bdata analyst\b') THEN 'Data Analyst'
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'analyste data') THEN 'Data Analyst'
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'\bdata scientist\b') THEN 'Data Scientist'
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'\bml engineer\b|\bmachine learning engineer\b') THEN 'ML Engineer'
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'\bbi\b|\bbusiness intelligence\b|\banalyst\b') THEN 'BI / Analytics'
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'\bdata\b') THEN 'Autre métier data'
      ELSE 'Autre'
    END AS job_family,
    CASE
      WHEN job_title IS NULL THEN FALSE
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'\bdata engineer\b') THEN TRUE
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'ing[eé]nieur data') THEN TRUE
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'\bdatabricks\b') THEN TRUE
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'\betl\b') THEN TRUE
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'big data') THEN TRUE
      WHEN REGEXP_CONTAINS(LOWER(job_title), r'cloud data') THEN TRUE
      ELSE FALSE
    END AS is_data_engineer,
    CASE
      WHEN alternance_source = TRUE THEN TRUE
      WHEN REGEXP_CONTAINS(LOWER(COALESCE(job_title, '')), r'\balternance\b|\bapprentissage\b|\bapprenti\b|\bcontrat de professionnalisation\b') THEN TRUE
      WHEN REGEXP_CONTAINS(LOWER(COALESCE(type_contrat_libelle, '')), r'\balternance\b|\bapprentissage\b|\bapprenti\b|\bcontrat de professionnalisation\b') THEN TRUE
      WHEN REGEXP_CONTAINS(LOWER(COALESCE(type_contrat, '')), r'\balternance\b|\bapprentissage\b|\bapprenti\b|\bcontrat de professionnalisation\b') THEN TRUE
      WHEN REGEXP_CONTAINS(LOWER(COALESCE(nature_contrat, '')), r'\balternance\b|\bapprentissage\b|\bapprenti\b|\bcontrat de professionnalisation\b') THEN TRUE
      ELSE FALSE
    END AS alternance,
    CASE WHEN job_title IS NOT NULL THEN TRUE ELSE FALSE END AS has_job_title,
    CASE WHEN company_name_display IS NOT NULL THEN TRUE ELSE FALSE END AS has_company,
    CASE WHEN city_std IS NOT NULL THEN TRUE ELSE FALSE END AS has_city,
    CASE WHEN departement_std IS NOT NULL THEN TRUE ELSE FALSE END AS has_departement,
    CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN TRUE ELSE FALSE END AS has_coordinates,
    CASE WHEN salary_avg_min IS NOT NULL OR salary_avg_max IS NOT NULL THEN TRUE ELSE FALSE END AS has_salary,
    CASE WHEN sirene_matched = 1 THEN TRUE ELSE FALSE END AS has_sirene_match,
    {{ departement_libelle_case('departement_std') }} AS departement_libelle,
    CASE
      WHEN departement_std IS NULL THEN NULL
      ELSE CONCAT(
        departement_std,
        ' - ',
        COALESCE({{ departement_libelle_case('departement_std') }}, 'Inconnu')
      )
    END AS departement_label
  FROM location_std
)
SELECT
  offer_id,
  job_title,
  title_raw,
  intitule_raw,
  company_name_raw,
  company_name_clean,
  entreprise_nom,
  sirene_name,
  adzuna_company_name,
  company_name_display,
  company_name_std,
  sirene_matched,
  match_method,
  match_score,
  naf_match,
  code_naf_offre,
  code_naf_sirene,
  siren,
  siret,
  location_raw,
  location_name_raw,
  lieu_libelle,
  lieu_commune,
  postal_code,
  latitude,
  longitude,
  city_std,
  departement_std,
  departement_libelle,
  departement_label,
  location_secondary_std,
  type_contrat,
  type_contrat_libelle,
  nature_contrat,
  alternance,
  experience_exige,
  experience_libelle,
  duree_travail_libelle,
  nb_positions,
  salaire_libelle,
  salary_avg_min,
  salary_avg_max,
  salary_min_raw,
  salary_max_raw,
  salary_avg,
  salary_range,
  salary_bucket,
  rome_code,
  rome_libelle,
  appellation_libelle,
  code_naf_std,
  secteur_activite,
  secteur_activite_libelle,
  ft_description,
  entreprise_description,
  contract_group,
  experience_group,
  job_family,
  is_data_engineer,
  has_job_title,
  has_company,
  has_city,
  has_departement,
  has_coordinates,
  has_salary,
  has_sirene_match,
  enriched_at,
  last_seen_at,
  date_creation,
  date_actualisation,
  adzuna_latest_created_at,
  valid_from,
  valid_to,
  dl_insert_date,
  dl_update_date
FROM enriched
