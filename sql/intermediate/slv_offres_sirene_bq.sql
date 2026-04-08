-- SILVER / INTERMEDIATE - Offres + matching Sirene
CREATE OR REPLACE TABLE `datatalent-simplon.intermediate.slv_offres_sirene` AS
SELECT
  s.offer_id,
  s.source_offer_id,
  s.source_name,
  s.is_france_travail,
  s.is_adzuna,
  s.company_name,
  s.company_name_clean,
  s.location_name,
  s.title,
  s.rome_code,
  s.code_naf_offre,
  e.sirene_matched,
  e.match_method,
  e.match_score,
  e.naf_match,
  e.code_naf_sirene,
  e.sirene_name,
  e.siren,
  e.siret,
  s.ingested_at,
  s.last_seen_at
FROM `datatalent-simplon.staging.stg_offres_all` s
LEFT JOIN `datatalent-simplon.raw.offres_enriched_team` e
  ON e.offer_id = s.offer_id;

