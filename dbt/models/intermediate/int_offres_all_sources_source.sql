{{ config(materialized='table') }}

{% set offres_staging_relation = source('staging', 'offres_staging') %}
{% set offres_staging_columns = adapter.get_columns_in_relation(offres_staging_relation) %}
{% set offres_staging_column_names = offres_staging_columns | map(attribute='name') | map('lower') | list %}
{% set has_delete_update = 'delete_update' in offres_staging_column_names %}

SELECT
  oe.*,
  os.id_offre,
  os.intitule,
  os.description AS ft_description,
  os.date_creation,
  os.date_actualisation AS update_date,
  {% if has_delete_update %}
  COALESCE(os.delete_update, os.date_actualisation) AS delete_update,
  {% else %}
  os.date_actualisation AS delete_update,
  {% endif %}
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
  CAST(NULL AS STRING) AS adzuna_company_name,
  CAST(NULL AS INT64) AS adzuna_job_count,
  CAST(NULL AS FLOAT64) AS adzuna_avg_salary_min,
  CAST(NULL AS FLOAT64) AS adzuna_avg_salary_max,
  CAST(NULL AS FLOAT64) AS adzuna_min_salary_min,
  CAST(NULL AS FLOAT64) AS adzuna_max_salary_max,
  CAST(NULL AS TIMESTAMP) AS adzuna_latest_created_at
FROM {{ ref('stg_offres_enriched_clean') }} oe
LEFT JOIN {{ source('staging', 'offres_staging') }} os
  ON oe.offer_id = os.id_offre
