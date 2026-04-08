# SQL models (Medallion)

Ce dossier prepare la structure demandee par le brief:

- `sql/staging/` : nettoyage et typage par source
- `sql/intermediate/` : jointures metier entre sources
- `sql/marts/` : tables analytiques pour la BI
- `sql/tests/` : controles qualite SQL (not null, unique, accepted values)

Convention proposee:
- prefixe `stg_` pour staging
- prefixe `int_` pour intermediate
- prefixe `dim_` et `fact_` pour marts
- prefixe `test_` pour les tests qualite

Execution locale:
- pour l'exploration, les requetes historiques sont dans `docs/queries_exploration_jointures.sql`
- les nouveaux modeles SQL versionnes seront incrementes ici par couche
