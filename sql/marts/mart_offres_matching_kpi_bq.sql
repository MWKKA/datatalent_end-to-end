-- MART - KPI matching par source
CREATE OR REPLACE TABLE `datatalent-simplon.marts.mart_offres_matching_kpi` AS
SELECT
  source_name,
  COUNT(*) AS total_offres,
  SUM(CASE WHEN sirene_matched = 1 THEN 1 ELSE 0 END) AS matched_offres,
  ROUND(100 * SAFE_DIVIDE(SUM(CASE WHEN sirene_matched = 1 THEN 1 ELSE 0 END), COUNT(*)), 1) AS matched_pct
FROM `datatalent-simplon.intermediate.slv_offres_sirene`
GROUP BY source_name;

