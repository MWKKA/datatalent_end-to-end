{{ config(materialized='table') }}

SELECT
  COALESCE(match_method, 'Non renseigné') AS match_method,
  COUNT(*) AS nb_offres,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_offres,
  ROUND(AVG(match_score), 2) AS avg_match_score,
  COUNTIF(sirene_matched = 1) AS nb_match_ok
FROM {{ ref('mart_offres_clean') }}
GROUP BY COALESCE(match_method, 'Non renseigné')
ORDER BY nb_offres DESC
