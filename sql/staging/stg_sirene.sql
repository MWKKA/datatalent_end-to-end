-- staging sirene
-- objectif: exposer les colonnes sirene minimales pour les jointures metier.

-- TODO:
-- - selectionner seulement les champs necessaires
-- - harmoniser les noms de colonnes

-- exemple d'intention:
-- CREATE OR REPLACE VIEW stg_sirene_unite_legale AS
-- SELECT
--   siren,
--   denominationUniteLegale AS denomination,
--   activitePrincipaleUniteLegale AS code_naf
-- FROM raw_sirene_unite_legale;
