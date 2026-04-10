{{ config(materialized='table') }}

SELECT
  oe.*,
  os.id_offre,
  os.intitule,
  os.description AS ft_description,
  os.date_creation,
  COALESCE(os.date_actualisation, oe.last_seen_at, oe.enriched_at) AS update_date,
  os.lieu_libelle,
  os.lieu_latitude,
  os.lieu_longitude,
  os.lieu_code_postal,
  os.lieu_commune,
  os.rome_code,
  os.rome_libelle,
  os.appellation_libelle,
  os.entreprise_nom,
  os.entreprise_description,
  os.type_contrat,
  os.type_contrat_libelle,
  os.nature_contrat,
  os.experience_exige,
  os.experience_libelle,
  os.duree_travail_libelle,
  os.alternance,
  os.nombre_postes,
  os.salaire_libelle,
  os.code_naf,
  os.secteur_activite,
  os.secteur_activite_libelle,
  ac.company_name AS adzuna_company_name,
  ac.adzuna_job_count,
  ac.adzuna_avg_salary_min,
  ac.adzuna_avg_salary_max,
  ac.adzuna_min_salary_min,
  ac.adzuna_max_salary_max,
  ac.adzuna_latest_created_at
FROM {{ ref('stg_offres_enriched_clean') }} AS oe
LEFT JOIN {{ ref('stg_offres_staging') }} AS os
  ON oe.offer_id = os.id_offre
LEFT JOIN {{ ref('int_adzuna_company_metrics') }} AS ac
  ON oe.company_name_clean = ac.company_name_clean
