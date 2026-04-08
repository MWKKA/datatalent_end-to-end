# datatalent_end-to-end

Pipeline de données DataTalent : **où recrute-t-on des Data Engineers en France, dans quelles entreprises et à quels salaires ?**

Sources : API France Travail (offres), base Sirene INSEE (entreprises), API Géo (référentiels).

## Démarrer en local (sans cloud)

Pour tout faire en local, voir les données et identifier les clés (PK/FK) avec DBeaver :

→ **[docs/SETUP_LOCAL.md](docs/SETUP_LOCAL.md)** — étapes une par une (env, téléchargement Sirene/Géo, base locale, DBeaver, jointures).

## Structure SQL (Medallion)

- `sql/staging/` : modèles de nettoyage/typage par source
- `sql/intermediate/` : modèles de jointure métier
- `sql/marts/` : modèles analytiques (dimensions + faits)
- `sql/tests/` : tests qualité SQL

Voir `sql/README.md` pour la convention de nommage et la feuille de route.