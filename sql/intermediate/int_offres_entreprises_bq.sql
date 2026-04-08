CREATE OR REPLACE TABLE `datatalent-simplon.intermediate.int_offres_entreprises` AS
SELECT
  s.offer_id,
  s.title,
  s.company_name,
  s.company_name_clean,
  s.location_name,
  s.sirene_matched,
  s.match_method,
  s.match_score,
  s.naf_match,
  s.code_naf_offre,
  s.code_naf_sirene,
  s.sirene_name,
  s.siren,
  s.siret,
  s.enriched_at,
  s.last_seen_at
FROM `datatalent-simplon.staging.offres_sirene_clean` s;

