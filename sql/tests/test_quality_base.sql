-- tests qualite SQL (socle)
-- ces requetes doivent retourner 0 ligne si tout est correct.

-- TODO unique id offres
-- SELECT id, COUNT(*) AS n
-- FROM raw_france_travail_offres
-- GROUP BY id
-- HAVING COUNT(*) > 1;

-- TODO not null offre_id dans table de matching
-- SELECT *
-- FROM offres_avec_siren
-- WHERE offre_id IS NULL;

-- TODO accepted values type_contrat (a definir selon domaine)
