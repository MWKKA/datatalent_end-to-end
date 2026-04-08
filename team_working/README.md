# Team working - pipeline API Sirene (sandbox)

Espace isole pour tester rapidement une approche equipe:

1. Ingestion offres (raw)
2. Enrichissement entreprises via API Sirene

Base dediee:
- `team_working/db/team_working.db`

## Scripts

- `team_working/scripts/init_team_working_db.py`
  - cree la base et les tables
- `team_working/scripts/ingest_offres_raw.py`
  - appelle les APIs France Travail + Adzuna et charge les offres en raw dans `raw_offres_team`
  - ajoute les flags source (`source_name`, `is_france_travail`, `is_adzuna`)
- `team_working/scripts/enrich_companies_sirene.py`
  - lit les entreprises uniques de `raw_offres_team`
  - appelle l'API Sirene
  - stocke le brut API dans `raw_sirene_results_team`
  - cree un mapping offre -> entreprise enrichie dans `offres_enriched_team`

## Utilisation

Depuis la racine du projet:

```bash
uv run python team_working/scripts/init_team_working_db.py
uv run python team_working/scripts/ingest_offres_raw.py --mots-cles "data engineer" --max-results 1150
uv run python team_working/scripts/enrich_companies_sirene.py
# optionnel: cibler explicitement un run
# uv run python team_working/scripts/enrich_companies_sirene.py --run-ts "2026-03-14T12:34:56+00:00"
# par defaut, le script enrichit toutes les offres en base (--scope all)
# optionnel: limiter au dernier run
# uv run python team_working/scripts/enrich_companies_sirene.py --scope run
```

Prerequis:
- `.env` configure avec `FRANCE_TRAVAIL_CLIENT_ID` et `FRANCE_TRAVAIL_CLIENT_SECRET`
- `.env` configure aussi `ADZUNA_APP_ID` et `ADZUNA_APP_KEY`

Notes importantes:
- Le script offres affiche:
  - `run_ts`,
  - lignes recuperees depuis les APIs (FT + Adzuna),
  - detail par source,
  - ids offre uniques du run,
  - offres uniques effectivement en base (apres dedoublonnage par `offer_id`).
- Le script offres applique un filtre de securite "data engineer" sur intitule/description.
- Le code NAF de l'offre est extrait et stocke (`code_naf_offre`).
- Le script Sirene gere maintenant les URLs trop longues (`414`) en divisant automatiquement les batches.
- Le matching Sirene utilise un score (nom normalise + bonus NAF) avec un seuil minimal.
- Le script Sirene affiche des metriques:
  - `[run courant] matched/total` (sur le `run_ts` cible)
  - `[global] matched/total` (historique cumule)

## Diagnostic qualite SQL

Fichier:
- `team_working/sql/quality_checks.sql`

Ce script donne:
- KPI global de matching
- couverture NAF
- top entreprises non match
- matchs faibles (score bas)
- repartition des methodes de match
- variantes de noms d'entreprise (ex: DATAGALAXY vs DATA GALAXY)
