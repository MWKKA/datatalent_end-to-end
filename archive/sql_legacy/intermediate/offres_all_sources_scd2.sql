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

