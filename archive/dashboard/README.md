# Dashboard Streamlit

Lit les tables **BigQuery** du dataset `marts` (KPI, familles, géo, détail).

## Lancer

Depuis la racine du dépôt (avec authentification GCP active) :

```bash
uv run streamlit run archive/dashboard/app.py
```

Variables utiles (fichier `.env` à la racine) :

- `BQ_PROJECT_ID` (défaut : `datatalent-simplon`)

## Prérequis

- Tables `marts` à jour : `uv run dbt run --select marts` dans `dbt/`
- Credentials : `gcloud auth application-default login` (ou compte de service)
