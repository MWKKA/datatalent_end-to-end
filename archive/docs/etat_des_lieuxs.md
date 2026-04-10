----- SQL UTILISE EN PROD —-----------------------------------------------------------------------------------------

SQL RAW VERS STAGING : 


ADZUNA 

 CREATE OR REPLACE TABLE `datatalent-simplon.staging.adzuna_jobs_clean` AS
SELECT
  job_id,
  title,

  company_name,

  TRIM(
    REGEXP_REPLACE(
      LOWER(company_name),
      r'[^a-z0-9]',
      ''
    )
  ) AS company_name_clean,

  location_display_name,
  latitude,
  longitude,
  salary_min,
  salary_max,
  salary_is_predicted,
  created AS created_at,
  description,
  redirect_url,
  search_term,
  collected_at,
  ingestion_timestamp

FROM `datatalent-simplon.raw.adzuna_jobs`

WHERE job_id IS NOT NULL
  AND company_name IS NOT NULL
  AND LOWER(title) LIKE "%data engineer%" 


_____________________________________________________________________

CREATE OR REPLACE TABLE `datatalent-simplon.staging.offres_enriched_clean` AS
SELECT
  e.offer_id,
  o.intitule AS title,
  e.company_name,
  TRIM(REGEXP_REPLACE(LOWER(e.company_name), r'[^a-z0-9]', '')) AS company_name_clean,
  o.location_name,
  e.sirene_matched,
  e.match_method,
  e.match_score,
  e.naf_match,
  e.code_naf_offre,
  e.code_naf_sirene,
  e.sirene_name,
  e.siren,
  e.siret,
  e.enriched_at,
  e.last_seen_at
FROM `datatalent-simplon.raw.offres_enriched_team` e
LEFT JOIN `datatalent-simplon.raw.raw_offres_team` o
  ON o.offer_id = e.offer_id
WHERE e.offer_id IS NOT NULL;



CREATE OR REPLACE TABLE `datatalent-simplon.staging.offres_france_travail` AS
SELECT
  JSON_VALUE(raw_json, '$.id') AS offer_id,
  JSON_VALUE(raw_json, '$.intitule') AS job_title,
  JSON_VALUE(raw_json, '$.description') AS description,
  TIMESTAMP(JSON_VALUE(raw_json, '$.dateCreation')) AS date_creation,
  TIMESTAMP(JSON_VALUE(raw_json, '$.dateActualisation')) AS date_actualisation,
  JSON_VALUE(raw_json, '$.entreprise.nom') AS company_name,
  JSON_VALUE(raw_json, '$.romeCode') AS rome_code,
  JSON_VALUE(raw_json, '$.romeLibelle') AS rome_label,
  JSON_VALUE(raw_json, '$.typeContrat') AS contract_type,
  JSON_VALUE(raw_json, '$.typeContratLibelle') AS contract_type_label,
  JSON_VALUE(raw_json, '$.experienceLibelle') AS experience_label,
  JSON_VALUE(raw_json, '$.salaire.libelle') AS salary_raw,
  JSON_VALUE(raw_json, '$.lieuTravail.codePostal') AS postal_code,
  JSON_VALUE(raw_json, '$.lieuTravail.commune') AS commune_code,
  JSON_VALUE(raw_json, '$.lieuTravail.libelle') AS city_label,
  SAFE_CAST(JSON_VALUE(raw_json, '$.lieuTravail.latitude') AS FLOAT64) AS latitude,
  SAFE_CAST(JSON_VALUE(raw_json, '$.lieuTravail.longitude') AS FLOAT64) AS longitude,
  JSON_VALUE(raw_json, '$.codeNAF') AS naf_code,
  JSON_VALUE(raw_json, '$.secteurActiviteLibelle') AS sector_label,
  ingestion_timestamp,
  source_file
FROM `datatalent-simplon.raw.offres_raw`;





-- Intermediate SCD2 - table unifiee offres enrichies + FT + metrics Adzuna


-- 1) Source unifiee
CREATE TEMP TABLE source AS
SELECT
  oe.*,


  os.id_offre,
  os.intitule,
  os.description AS ft_description,
  os.date_creation,
  os.date_actualisation AS update_date,
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


FROM `datatalent-simplon.staging.offres_enriched_clean` oe
LEFT JOIN `datatalent-simplon.staging.offres_staging` os
  ON oe.offer_id = os.id_offre
LEFT JOIN `datatalent-simplon.intermediate.adzuna_company_metrics` ac
  ON oe.company_name_clean = ac.company_name_clean;


-- 2) Cree la table cible si elle n'existe pas encore
CREATE TABLE IF NOT EXISTS `datatalent-simplon.intermediate.offres_all_sources_scd2`
AS
SELECT
  source.*,
  CURRENT_TIMESTAMP() AS valid_from,
  CAST(NULL AS TIMESTAMP) AS valid_to,
  TRUE AS is_current,
  CURRENT_TIMESTAMP() AS dl_insert_date,
  CURRENT_TIMESTAMP() AS dl_update_date
FROM source
WHERE 1 = 0;


-- 3) Ferme les anciennes versions si la ligne a change
MERGE `datatalent-simplon.intermediate.offres_all_sources_scd2` T
USING source S
ON T.offer_id = S.offer_id
AND T.is_current = TRUE
WHEN MATCHED
  AND T.update_date IS DISTINCT FROM S.update_date
THEN
  UPDATE SET
    valid_to = CURRENT_TIMESTAMP(),
    is_current = FALSE,
    dl_update_date = CURRENT_TIMESTAMP();


-- 4) Insere les nouvelles lignes et les nouvelles versions
INSERT INTO `datatalent-simplon.intermediate.offres_all_sources_scd2`
SELECT
  S.*,
  CURRENT_TIMESTAMP() AS valid_from,
  CAST(NULL AS TIMESTAMP) AS valid_to,
  TRUE AS is_current,
  CURRENT_TIMESTAMP() AS dl_insert_date,
  CURRENT_TIMESTAMP() AS dl_update_date
FROM source S
LEFT JOIN `datatalent-simplon.intermediate.offres_all_sources_scd2` T
  ON S.offer_id = T.offer_id
  AND T.is_current = TRUE
WHERE
  T.offer_id IS NULL
  OR T.update_date IS DISTINCT FROM S.update_date;







CREATE OR REPLACE VIEW `datatalent-simplon.intermediate.v_offres_all_sources_current` AS
SELECT *
FROM `datatalent-simplon.intermediate.offres_all_sources_scd2`
WHERE is_current = TRUE;

2) Mart central clean
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_offres_clean` AS
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


   -- alternance source brute conservée uniquement pour logique interne si besoin
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


 FROM `datatalent-simplon.intermediate.v_offres_all_sources_current`
),


location_std AS (
 SELECT
   *,
   CASE
     WHEN LOWER(COALESCE(location_raw, '')) = 'france' THEN NULL
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
   END AS location_secondary_std
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
     WHEN LOWER(COALESCE(type_contrat_libelle, type_contrat, nature_contrat, '')) LIKE '%cdi%' THEN 'CDI'
     WHEN LOWER(COALESCE(type_contrat_libelle, type_contrat, nature_contrat, '')) LIKE '%cdd%' THEN 'CDD'
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


   -- alternance reconstruite directement dans le mart
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
   CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN TRUE ELSE FALSE END AS has_coordinates,
   CASE WHEN salary_avg_min IS NOT NULL OR salary_avg_max IS NOT NULL THEN TRUE ELSE FALSE END AS has_salary,
   CASE WHEN sirene_matched = 1 THEN TRUE ELSE FALSE END AS has_sirene_match


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
FROM enriched;


3) Mart focus Data Engineer
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_offres_data_engineer` AS
SELECT *
FROM `datatalent-simplon.marts.mart_offres_clean`
WHERE is_data_engineer = TRUE;

4) KPI globaux
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_kpi_global` AS
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
FROM `datatalent-simplon.marts.mart_offres_clean`;

5) KPI Data Engineer
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_kpi_data_engineer` AS
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
FROM `datatalent-simplon.marts.mart_offres_data_engineer`;

6) Répartition par famille métier
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_jobs_family` AS
SELECT
 job_family,
 COUNT(*) AS nb_offres,
 ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_offres,
 COUNT(DISTINCT company_name_std) AS nb_entreprises,
 COUNT(DISTINCT city_std) AS nb_villes,
 ROUND(AVG(salary_avg), 2) AS salaire_moyen,
 APPROX_QUANTILES(salary_avg, 100)[OFFSET(50)] AS salaire_median
FROM `datatalent-simplon.marts.mart_offres_clean`
GROUP BY job_family
ORDER BY nb_offres DESC;

7) Géographie tous métiers
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_jobs_geo` AS
SELECT
 city_std AS ville,
 ANY_VALUE(postal_code) AS code_postal,
 ANY_VALUE(latitude) AS latitude,
 ANY_VALUE(longitude) AS longitude,
 COUNT(*) AS nb_offres,
 COUNT(DISTINCT company_name_std) AS nb_entreprises,
 COUNTIF(has_salary) AS nb_offres_avec_salaire,
 ROUND(AVG(salary_avg), 2) AS salaire_moyen,
 APPROX_QUANTILES(salary_avg, 100)[OFFSET(50)] AS salaire_median,
 MIN(salary_avg) AS salaire_min,
 MAX(salary_avg) AS salaire_max
FROM `datatalent-simplon.marts.mart_offres_clean`
WHERE city_std IS NOT NULL
GROUP BY city_std
ORDER BY nb_offres DESC;

8) Géographie Data Engineer
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_jobs_geo_data_engineer` AS
SELECT
 city_std AS ville,
 ANY_VALUE(postal_code) AS code_postal,
 ANY_VALUE(latitude) AS latitude,
 ANY_VALUE(longitude) AS longitude,
 COUNT(*) AS nb_offres,
 COUNT(DISTINCT company_name_std) AS nb_entreprises,
 COUNTIF(has_salary) AS nb_offres_avec_salaire,
 ROUND(AVG(salary_avg), 2) AS salaire_moyen,
 APPROX_QUANTILES(salary_avg, 100)[OFFSET(50)] AS salaire_median,
 MIN(salary_avg) AS salaire_min,
 MAX(salary_avg) AS salaire_max
FROM `datatalent-simplon.marts.mart_offres_data_engineer`
WHERE city_std IS NOT NULL
GROUP BY city_std
ORDER BY nb_offres DESC;

9) Entreprises tous métiers
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_jobs_company` AS
SELECT
 company_name_std,
 ANY_VALUE(company_name_display) AS company_name_display,
 ANY_VALUE(siren) AS siren,
 COUNT(*) AS nb_offres,
 COUNT(DISTINCT city_std) AS nb_villes,
 COUNTIF(has_salary) AS nb_offres_avec_salaire,
 ROUND(AVG(salary_avg), 2) AS salaire_moyen,
 APPROX_QUANTILES(salary_avg, 100)[OFFSET(50)] AS salaire_median,
 MIN(salary_avg) AS salaire_min,
 MAX(salary_avg) AS salaire_max
FROM `datatalent-simplon.marts.mart_offres_clean`
WHERE company_name_std IS NOT NULL
GROUP BY company_name_std
ORDER BY nb_offres DESC;

10) Entreprises Data Engineer
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_jobs_company_data_engineer` AS
SELECT
 company_name_std,
 ANY_VALUE(company_name_display) AS company_name_display,
 ANY_VALUE(siren) AS siren,
 COUNT(*) AS nb_offres,
 COUNT(DISTINCT city_std) AS nb_villes,
 COUNTIF(has_salary) AS nb_offres_avec_salaire,
 ROUND(AVG(salary_avg), 2) AS salaire_moyen,
 APPROX_QUANTILES(salary_avg, 100)[OFFSET(50)] AS salaire_median,
 MIN(salary_avg) AS salaire_min,
 MAX(salary_avg) AS salaire_max
FROM `datatalent-simplon.marts.mart_offres_data_engineer`
WHERE company_name_std IS NOT NULL
GROUP BY company_name_std
ORDER BY nb_offres DESC;

11) Salaires par ville tous métiers
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_jobs_salary_by_city` AS
SELECT
 city_std AS ville,
 COUNT(*) AS nb_offres,
 COUNTIF(has_salary) AS nb_offres_avec_salaire,
 ROUND(AVG(salary_avg), 2) AS salaire_moyen,
 APPROX_QUANTILES(salary_avg, 100)[OFFSET(50)] AS salaire_median,
 MIN(salary_avg) AS salaire_min,
 MAX(salary_avg) AS salaire_max
FROM `datatalent-simplon.marts.mart_offres_clean`
WHERE city_std IS NOT NULL
GROUP BY city_std
HAVING COUNT(*) >= 3
ORDER BY salaire_moyen DESC;

12) Salaires par ville Data Engineer
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_jobs_salary_by_city_data_engineer` AS
SELECT
 city_std AS ville,
 COUNT(*) AS nb_offres,
 COUNTIF(has_salary) AS nb_offres_avec_salaire,
 ROUND(AVG(salary_avg), 2) AS salaire_moyen,
 APPROX_QUANTILES(salary_avg, 100)[OFFSET(50)] AS salaire_median,
 MIN(salary_avg) AS salaire_min,
 MAX(salary_avg) AS salaire_max
FROM `datatalent-simplon.marts.mart_offres_data_engineer`
WHERE city_std IS NOT NULL
GROUP BY city_std
HAVING COUNT(*) >= 2
ORDER BY salaire_moyen DESC;

13) Buckets de salaire tous métiers
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_jobs_salary_bucket` AS
SELECT
 salary_bucket,
 COUNT(*) AS nb_offres,
 ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_offres
FROM `datatalent-simplon.marts.mart_offres_clean`
GROUP BY salary_bucket
ORDER BY
 CASE salary_bucket
   WHEN '<35k' THEN 1
   WHEN '35k-45k' THEN 2
   WHEN '45k-55k' THEN 3
   WHEN '55k-65k' THEN 4
   WHEN '65k+' THEN 5
   ELSE 6
 END;

14) Buckets de salaire Data Engineer
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_jobs_salary_bucket_data_engineer` AS
SELECT
 salary_bucket,
 COUNT(*) AS nb_offres,
 ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_offres
FROM `datatalent-simplon.marts.mart_offres_data_engineer`
GROUP BY salary_bucket
ORDER BY
 CASE salary_bucket
   WHEN '<35k' THEN 1
   WHEN '35k-45k' THEN 2
   WHEN '45k-55k' THEN 3
   WHEN '55k-65k' THEN 4
   WHEN '65k+' THEN 5
   ELSE 6
 END;

15) Contrats tous métiers
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_jobs_contract` AS
SELECT
 contract_group,
 COUNT(*) AS nb_offres,
 ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_offres,
 ROUND(AVG(salary_avg), 2) AS salaire_moyen,
 APPROX_QUANTILES(salary_avg, 100)[OFFSET(50)] AS salaire_median
FROM `datatalent-simplon.marts.mart_offres_clean`
GROUP BY contract_group
ORDER BY nb_offres DESC;

16) Contrats Data Engineer
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_jobs_contract_data_engineer` AS
SELECT
 contract_group,
 COUNT(*) AS nb_offres,
 ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_offres,
 ROUND(AVG(salary_avg), 2) AS salaire_moyen,
 APPROX_QUANTILES(salary_avg, 100)[OFFSET(50)] AS salaire_median
FROM `datatalent-simplon.marts.mart_offres_data_engineer`
GROUP BY contract_group
ORDER BY nb_offres DESC;

17) Expérience tous métiers
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_jobs_experience` AS
SELECT
 experience_group,
 COUNT(*) AS nb_offres,
 ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_offres,
 ROUND(AVG(salary_avg), 2) AS salaire_moyen,
 APPROX_QUANTILES(salary_avg, 100)[OFFSET(50)] AS salaire_median
FROM `datatalent-simplon.marts.mart_offres_clean`
GROUP BY experience_group
ORDER BY nb_offres DESC;

18) Expérience Data Engineer
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_jobs_experience_data_engineer` AS
SELECT
 experience_group,
 COUNT(*) AS nb_offres,
 ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_offres,
 ROUND(AVG(salary_avg), 2) AS salaire_moyen,
 APPROX_QUANTILES(salary_avg, 100)[OFFSET(50)] AS salaire_median
FROM `datatalent-simplon.marts.mart_offres_data_engineer`
GROUP BY experience_group
ORDER BY nb_offres DESC;

19) Qualité de données
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_data_quality` AS
SELECT
 'job_title' AS colonne,
 COUNT(*) AS nb_total,
 COUNTIF(job_title IS NULL) AS nb_null,
 ROUND(100 * COUNTIF(job_title IS NULL) / COUNT(*), 2) AS pct_null
FROM `datatalent-simplon.marts.mart_offres_clean`

UNION ALL
SELECT 'company_name_display', COUNT(*), COUNTIF(company_name_display IS NULL), ROUND(100 * COUNTIF(company_name_display IS NULL) / COUNT(*), 2)
FROM `datatalent-simplon.marts.mart_offres_clean`

UNION ALL
SELECT 'company_name_std', COUNT(*), COUNTIF(company_name_std IS NULL), ROUND(100 * COUNTIF(company_name_std IS NULL) / COUNT(*), 2)
FROM `datatalent-simplon.marts.mart_offres_clean`

UNION ALL
SELECT 'city_std', COUNT(*), COUNTIF(city_std IS NULL), ROUND(100 * COUNTIF(city_std IS NULL) / COUNT(*), 2)
FROM `datatalent-simplon.marts.mart_offres_clean`

UNION ALL
SELECT 'salary_avg', COUNT(*), COUNTIF(salary_avg IS NULL), ROUND(100 * COUNTIF(salary_avg IS NULL) / COUNT(*), 2)
FROM `datatalent-simplon.marts.mart_offres_clean`

UNION ALL
SELECT 'contract_group', COUNT(*), COUNTIF(contract_group IS NULL), ROUND(100 * COUNTIF(contract_group IS NULL) / COUNT(*), 2)
FROM `datatalent-simplon.marts.mart_offres_clean`

UNION ALL
SELECT 'experience_group', COUNT(*), COUNTIF(experience_group IS NULL), ROUND(100 * COUNTIF(experience_group IS NULL) / COUNT(*), 2)
FROM `datatalent-simplon.marts.mart_offres_clean`

UNION ALL
SELECT 'job_family', COUNT(*), COUNTIF(job_family IS NULL), ROUND(100 * COUNTIF(job_family IS NULL) / COUNT(*), 2)
FROM `datatalent-simplon.marts.mart_offres_clean`

UNION ALL
SELECT 'siren', COUNT(*), COUNTIF(siren IS NULL), ROUND(100 * COUNTIF(siren IS NULL) / COUNT(*), 2)
FROM `datatalent-simplon.marts.mart_offres_clean`

UNION ALL
SELECT 'latitude', COUNT(*), COUNTIF(latitude IS NULL), ROUND(100 * COUNTIF(latitude IS NULL) / COUNT(*), 2)
FROM `datatalent-simplon.marts.mart_offres_clean`

UNION ALL
SELECT 'longitude', COUNT(*), COUNTIF(longitude IS NULL), ROUND(100 * COUNTIF(longitude IS NULL) / COUNT(*), 2)
FROM `datatalent-simplon.marts.mart_offres_clean`;

20) Qualité matching SIRENE
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_sirene_quality` AS
SELECT
 COALESCE(match_method, 'Non renseigné') AS match_method,
 COUNT(*) AS nb_offres,
 ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_offres,
 ROUND(AVG(match_score), 2) AS avg_match_score,
 COUNTIF(sirene_matched = 1) AS nb_match_ok
FROM `datatalent-simplon.marts.mart_offres_clean`
GROUP BY COALESCE(match_method, 'Non renseigné')
ORDER BY nb_offres DESC;

21) Entreprises × familles métier
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_company_job_family` AS
SELECT
 company_name_std,
 ANY_VALUE(company_name_display) AS company_name_display,
 job_family,
 COUNT(*) AS nb_offres,
 ROUND(AVG(salary_avg), 2) AS salaire_moyen
FROM `datatalent-simplon.marts.mart_offres_clean`
WHERE company_name_std IS NOT NULL
GROUP BY company_name_std, job_family
ORDER BY nb_offres DESC;

22) Villes × familles métier
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_city_job_family` AS
SELECT
 city_std AS ville,
 job_family,
 COUNT(*) AS nb_offres,
 ROUND(AVG(salary_avg), 2) AS salaire_moyen
FROM `datatalent-simplon.marts.mart_offres_clean`
WHERE city_std IS NOT NULL
GROUP BY city_std, job_family
ORDER BY nb_offres DESC;



22. Colonnes à garder pour le dashboard
Dans mart_offres_clean, les plus utiles sont :
offer_id
job_title
job_family
is_data_engineer
company_name_display
company_name_std
siren
city_std
postal_code
latitude
longitude
salary_avg
salary_bucket
contract_group
experience_group
alternance
nb_positions
sirene_matched
match_method
match_score
