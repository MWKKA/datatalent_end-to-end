-- SILVER / INTERMEDIATE - Offres harmonisees + matching Sirene + referentiel geo
-- Sources:
-- - staging.stg_offres_all_sources
-- - raw.offres_enriched_team
-- - staging.dim_commune / dim_departement / dim_region
--
-- Strategie de rapprochement Sirene:
-- 1) priorite au match par offer_id
-- 2) fallback par company_name_clean + meilleur match_score
-- Le QUALIFY evite les doublons (1 ligne finale par offre staging).

CREATE OR REPLACE TABLE `datatalent-simplon.intermediate.slv_offres_sirene_geo` AS
WITH enriched_norm AS (
  SELECT
    e.*,
    TRIM(REGEXP_REPLACE(LOWER(e.company_name), r'[^a-z0-9]', '')) AS company_name_clean_enriched,
    CASE
      WHEN STARTS_WITH(e.offer_id, 'adzuna_') THEN 'adzuna'
      ELSE 'france_travail'
    END AS source_name_enriched
  FROM `datatalent-simplon.raw.offres_enriched_team` e
),
joined AS (
  SELECT
    s.offer_id,
    s.source_name,
    s.is_france_travail,
    s.is_adzuna,
    s.company_name,
    s.company_name_clean,
    s.title,
    s.description,
    s.rome_code,
    s.code_naf_offre,
    s.location_name,
    s.commune_code,
    s.code_postal,
    s.created_at,
    s.ingestion_timestamp,
    e.sirene_matched,
    e.match_method,
    e.match_score,
    e.naf_match,
    e.code_naf_sirene,
    e.sirene_name,
    e.siren,
    e.siret,
    e.enriched_at,
    e.last_seen_at AS enriched_last_seen_at,
    c.commune_name AS commune_nom,
    c.dep_code AS departement_code,
    d.dep_nom AS departement_nom,
    r.reg_code AS region_code,
    r.reg_nom AS region_nom,
    CASE
      WHEN e.offer_id = s.offer_id THEN 1
      WHEN s.source_name = 'adzuna' AND e.offer_id = CONCAT('adzuna_', s.offer_id) THEN 1
      ELSE 0
    END AS is_offer_id_match
  FROM `datatalent-simplon.staging.stg_offres_all_sources` s
  LEFT JOIN enriched_norm e
    ON (
      e.offer_id = s.offer_id
      OR (s.source_name = 'adzuna' AND e.offer_id = CONCAT('adzuna_', s.offer_id))
      OR (
        e.source_name_enriched = s.source_name
        AND e.company_name_clean_enriched = s.company_name_clean
        AND e.company_name_clean_enriched IS NOT NULL
        AND e.company_name_clean_enriched <> ''
      )
    )
  LEFT JOIN `datatalent-simplon.staging.dim_commune` c
    ON c.commune_code_insee = s.commune_code
  LEFT JOIN `datatalent-simplon.staging.dim_departement` d
    ON d.dep_code = c.dep_code
  LEFT JOIN `datatalent-simplon.staging.dim_region` r
    ON r.reg_code = d.reg_code
)
SELECT
  offer_id,
  source_name,
  is_france_travail,
  is_adzuna,
  company_name,
  company_name_clean,
  title,
  description,
  rome_code,
  code_naf_offre,
  location_name,
  commune_code,
  code_postal,
  created_at,
  ingestion_timestamp,
  sirene_matched,
  match_method,
  match_score,
  naf_match,
  code_naf_sirene,
  sirene_name,
  siren,
  siret,
  enriched_at,
  enriched_last_seen_at,
  commune_nom,
  departement_code,
  departement_nom,
  region_code,
  region_nom
FROM joined
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY offer_id
  ORDER BY is_offer_id_match DESC, COALESCE(match_score, 0) DESC, enriched_last_seen_at DESC
) = 1;

