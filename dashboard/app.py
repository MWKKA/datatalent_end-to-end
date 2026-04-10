"""
Dashboard DataTalent — lecture des tables **marts** sur BigQuery (GCP).

Les données restent dans le cloud ; Streamlit s’exécute en local et utilise
les **Application Default Credentials** (même mécanisme que le client BQ Python).

Lancer depuis la racine du dépôt :
  uv run streamlit run dashboard/app.py

Prérequis :
  - `gcloud auth application-default login` (ou compte de service via GOOGLE_APPLICATION_CREDENTIALS)
  - `BQ_PROJECT_ID` dans `.env` (optionnel, défaut datatalent-simplon)
  - marts à jour : `cd dbt && uv run dbt build`
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery

_DASHBOARD_DIR = Path(__file__).resolve().parent
if str(_DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(_DASHBOARD_DIR))

from departements import format_departement_affichage, serie_departement_affiche

_REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_REPO_ROOT / ".env")
load_dotenv(_REPO_ROOT / ".env.example", override=False)

DEFAULT_PROJECT = os.getenv("BQ_PROJECT_ID", "datatalent-simplon")
DEFAULT_DATASET = "marts"


@st.cache_data(ttl=300)
def run_query(project_id: str, sql: str) -> pd.DataFrame:
    """Exécute une requête BigQuery et retourne un DataFrame."""
    client = bigquery.Client(project=project_id)
    return client.query(sql).to_dataframe()


def main() -> None:
    st.set_page_config(
        page_title="DataTalent — Offres",
        page_icon="📊",
        layout="wide",
    )
    st.title("DataTalent — Dashboard offres (BigQuery)")
    st.caption(
        "Connexion au dataset **marts** sur GCP. "
        "Authentification : ADC (`gcloud auth application-default login`)."
    )

    with st.sidebar:
        project_id = st.text_input("Projet GCP", value=DEFAULT_PROJECT)
        dataset = st.text_input("Dataset", value=DEFAULT_DATASET)
        max_rows = st.slider("Lignes max (table détail)", 50, 2000, 500, 50)
        only_de = st.checkbox("Uniquement Data Engineer (mart)", value=False)

    fq = f"`{project_id}.{dataset}`"

    st.subheader("Indicateurs globaux")
    try:
        kpi = run_query(
            project_id,
            f"SELECT * FROM {fq}.mart_kpi_global LIMIT 1",
        )
    except Exception as e:
        st.error(
            f"Impossible de lire mart_kpi_global : {e}\n\n"
            "Vérifie le projet, les droits BigQuery et que `gcloud auth application-default login` a été fait."
        )
        st.stop()

    if kpi.empty:
        st.warning("Table mart_kpi_global vide ou absente.")
    else:
        row = kpi.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Offres (total)", f"{int(row.get('nb_offres_total', 0) or 0):,}")
        c2.metric("Entreprises distinctes", f"{int(row.get('nb_entreprises_distinctes', 0) or 0):,}")
        c3.metric("Départements distincts", f"{int(row.get('nb_departements_distinctes', 0) or 0):,}")
        c4.metric("% match Sirene", f"{float(row.get('pct_match_sirene', 0) or 0):.1f}%")
        c5, c6, c7 = st.columns(3)
        c5.metric("% avec salaire", f"{float(row.get('pct_offres_avec_salaire', 0) or 0):.1f}%")
        c6.metric("Alternance", f"{int(row.get('nb_offres_alternance', 0) or 0):,}")
        if pd.notna(row.get("salaire_median")):
            c7.metric("Salaire médian (est.)", f"{float(row['salaire_median']):,.0f}")

    st.divider()

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Familles de métiers")
        try:
            fam = run_query(
                project_id,
                f"""
                SELECT job_family, nb_offres, pct_offres
                FROM {fq}.mart_jobs_family
                ORDER BY nb_offres DESC
                """,
            )
            if not fam.empty:
                st.bar_chart(fam.set_index("job_family")["nb_offres"])
                st.dataframe(fam, use_container_width=True, hide_index=True)
            else:
                st.info("mart_jobs_family vide.")
        except Exception as e:
            st.error(str(e))

    with col_b:
        st.subheader("Top départements")
        try:
            geo = run_query(
                project_id,
                f"""
                SELECT departement, nb_offres, salaire_moyen
                FROM {fq}.mart_jobs_geo
                ORDER BY nb_offres DESC
                LIMIT 15
                """,
            )
            if not geo.empty:
                geo = geo.copy()
                geo["departement"] = serie_departement_affiche(geo["departement"])
                chart_geo = (
                    alt.Chart(geo)
                    .mark_bar()
                    .encode(
                        x=alt.X("nb_offres:Q", title="Nombre d'offres"),
                        y=alt.Y("departement:N", sort="-x", title=""),
                        tooltip=[
                            alt.Tooltip("departement", title="Département"),
                            alt.Tooltip("nb_offres", title="Offres"),
                            alt.Tooltip("salaire_moyen", title="Salaire moyen", format=".0f"),
                        ],
                    )
                )
                st.altair_chart(chart_geo, use_container_width=True)
                st.dataframe(geo, use_container_width=True, hide_index=True)
            else:
                st.info("mart_jobs_geo vide.")
        except Exception as e:
            st.error(str(e))

    st.divider()

    st.subheader("Détail des offres (extrait)")
    table = "mart_offres_data_engineer" if only_de else "mart_offres_clean"
    try:
        detail = run_query(
            project_id,
            f"""
            SELECT
              offer_id,
              job_title,
              job_family,
              company_name_display,
              company_name_std,
              city_std,
              departement_label,
              postal_code,
              salary_avg,
              salary_bucket,
              contract_group,
              has_sirene_match,
              match_method,
              match_score
            FROM {fq}.{table}
            LIMIT {int(max_rows)}
            """,
        )
        if "departement_label" in detail.columns:
            detail = detail.copy()
            detail["departement_label"] = detail["departement_label"].apply(
                format_departement_affichage
            )
        st.dataframe(detail, use_container_width=True, height=400)
        csv_bytes = detail.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Télécharger CSV (extrait)",
            data=csv_bytes,
            file_name=f"{table}_extrait.csv",
            mime="text/csv",
        )
    except Exception as e:
        st.error(f"Lecture {table} : {e}")

    st.divider()
    st.caption(
        f"Projet : `{project_id}` · Dataset : `{dataset}` · "
        "Rafraîchir les données : `cd dbt && uv run dbt build`."
    )


if __name__ == "__main__":
    main()
