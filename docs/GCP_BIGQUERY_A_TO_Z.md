# GCP & BigQuery — Guide A à Z (quoi, comment, pourquoi)

Ce document décrit **pas à pas** comment transposer sur **Google Cloud (GCP)** et **BigQuery** tout ce qui a été fait en local : les trois sources (Géo, France Travail, Sirene), le chargement, le rapprochement offres–Sirene, et les jointures. Il précise **quoi** faire, **comment** le faire et **pourquoi**, avec une **maîtrise du flux** (ordre d’exécution, planification, coûts).

---

## 1. Ce qu’on a fait en local (rappel)

| Étape | Quoi | Comment | Pourquoi |
|-------|------|---------|----------|
| **Sources** | Géo (régions, dépts, communes), France Travail (offres "data engineer"), Sirene (établissements + unités légales) | Scripts Python (`scripts/ingestion/download_geo.py`, `scripts/ingestion/download_offres.py`, fichiers Parquet manuels) + `scripts/database/load_local_db.py` | Comprendre les données, identifier les PK/FK sans coût cloud |
| **Stockage** | Fichiers JSON/Parquet dans `data/raw/`, puis base SQLite | Dossiers + script SQLite | Explorer en SQL (DBeaver) |
| **Jointures** | Offres ↔ Géo par `commune_code` ; Offres ↔ Sirene par nom d’entreprise (table `offres_avec_siren`) | Requêtes SQL + script `scripts/matching/match_offres_sirene.py` (exact + préfixe) | L’API offres ne renvoie pas de SIRET ; il faut rapprocher par nom |
| **Résultat** | Schéma de jointures validé, 66/150 offres avec SIREN sur l’échantillon | Voir `docs/MODELE_JOINTURES.md` | Base pour le schéma en étoile et le dashboard |

En local, le flux est **manuel** : on lance les scripts à la main, on charge la base, on fait le match. Sur GCP, on vise le **même flux**, mais **automatisé**, **reproductible** et **maîtrisé** (planification, erreurs, coûts).

---

## 2. Pourquoi GCP et BigQuery

- **BigQuery** : entrepôt analytique (remplace SQLite), facturation au volume de données traitées par requête ; adapté aux grosses tables (ex. Sirene ~29 M lignes) et aux jointures en une seule requête (pas de boucle par offre).
- **Cloud Storage (GCS)** : stockage objet pour les fichiers raw (JSON, Parquet) avant chargement en BigQuery ; durable, versionnable, compatible avec les scripts d’ingestion existants.
- **Ordonnancement** : Cloud Scheduler + Cloud Functions (ou Cloud Run) pour lancer les ingestions et, si besoin, les jobs de transformation à date/heure fixe → **maîtrise du flux** dans le temps.
- **IaC** : Terraform (ou équivalent) pour recréer l’environnement (buckets, datasets, rôles) de façon reproductible.

---

## 3. Architecture cible (flux de données)

```
[Sources externes]
       │
       ├── API Géo ──────────────┐
       ├── API France Travail ───┼──► [Scripts d’ingestion] ──► GCS (raw/) ──► BigQuery raw_*
       └── Fichiers Sirene (Parquet) ─┘                              │
                                                                     ▼
                                                            [Transformations SQL]
                                                                     │
                    ┌────────────────────────────────────────────────┼────────────────────────────────────────────────┐
                    ▼                                                ▼                                                ▼
              staging_geo                                      staging_offres                              staging_sirene
              (régions, dépts, communes)                       (offres + match Sirene en 1 requête)        (UL, établissements)
                    │                                                │                                                │
                    └────────────────────────────────────────────────┼────────────────────────────────────────────────┘
                                                                     ▼
                                                            [Marts / schéma en étoile]
                                                                     │
                    fact_offres (id_offre, id_geo, id_entreprise, id_rome, id_contrat, date, salaire…)
                    + dim_geo, dim_entreprise, dim_rome, dim_contrat, dim_temps
                                                                     │
                                                                     ▼
                                                            [Dashboard BI]
```

**Maîtrise du flux** :  
1) Ingestion (Géo, Offres, Sirene) → écriture dans GCS puis chargement dans les tables **raw** BigQuery.  
2) Une fois les raw à jour, lancer les transformations SQL (staging puis marts). On peut enchaîner staging puis marts dans un même job (ex. Dataform / dbt) ou via des requêtes planifiées pour contrôler l’ordre.

---

## 4. Étapes A à Z sur GCP / BigQuery

### A. Projet GCP et facturation

1. Créer un **projet GCP** (console Cloud ou `gcloud projects create`).
2. Activer la **facturation** sur ce projet (obligatoire pour BigQuery et Cloud Run).
3. Activer les APIs : **BigQuery**, **Cloud Storage**, **Cloud Build** (si CI/CD), **Cloud Scheduler**, **Cloud Functions** (ou **Cloud Run**) selon le choix d’exécution des scripts.

**Pourquoi** : sans projet et facturation, impossible d’utiliser BigQuery et de planifier les jobs.

---

### B. Stockage objet (raw)

1. Créer un **bucket GCS** dédié aux données (ex. `datatalent-raw-<project_id>`).
2. Structure recommandée des préfixes (équivalent de `data/raw/`) :
   - `raw/geo/` (régions, départements, communes en JSON)
   - `raw/france_travail/` (offres en JSON ou Parquet)
   - `raw/sirene/` (fichiers Parquet StockEtablissement, StockUniteLegale)
3. Les **scripts d’ingestion** existants (`scripts/ingestion/download_geo.py`, `scripts/ingestion/download_offres.py`) sont adaptés pour écrire dans GCS au lieu du disque local (client `google-cloud-storage`, chemin `gs://bucket/raw/...`).

**Pourquoi** : une seule couche “raw” dans le cloud, reproductible et utilisable par BigQuery (load depuis GCS).

---

### C. BigQuery : dataset et tables raw

1. Créer un **dataset** BigQuery (ex. `datatalent`) dans le même projet.
2. Créer les **tables raw** (schéma aligné sur ce qu’on a en local) :
   - `raw_geo_regions`, `raw_geo_departements`, `raw_geo_communes`
   - `raw_france_travail_offres`
   - `raw_sirene_unite_legale`, `raw_sirene_etablissement`
3. **Chargement des données** :
   - **Géo / Offres** : depuis GCS (JSON ou Parquet) via `bq load` ou un job BigQuery Load (script Python avec `google-cloud-bigquery`).
   - **Sirene** : load direct des Parquet depuis GCS vers les tables raw (fichiers déjà dans le bucket après ingestion manuelle ou script).

**Pourquoi** : BigQuery devient la source unique de vérité ; les jointures et le “match offres–Sirene” se font en SQL, sans refaire une requête par offre.

---

### D. Rapprochement offres–Sirene (en une requête)

En local, le match était fait par un script Python (une requête par offre). En BigQuery, on fait **une seule** requête (ou une vue / table matérialisée) :

1. **Normalisation du nom** : `LOWER(TRIM(entreprise_nom))` côté offres, `LOWER(TRIM(denominationUniteLegale))` côté Sirene.
2. **Match exact** : `LEFT JOIN` sur ces champs normalisés.
3. **Match par préfixe** (fallback) : pour les offres sans match exact, utiliser une deuxième jointure ou une sous-requête avec `LIKE nom_base || '%'` (en prenant soin de définir “nom_base” comme en local, ex. sans forme juridique).
4. **Dédoublonnage** : si plusieurs SIREN pour un même nom, garder un seul enregistrement par offre (ex. `QUALIFY ROW_NUMBER() OVER (PARTITION BY id_offre ORDER BY siren) = 1` ou `ANY_VALUE(siren)`).
5. Écrire le résultat dans une table **`offres_avec_siren`** (ou vue) dans le même dataset.

**Pourquoi** : un seul scan des grosses tables → coût et temps maîtrisés, pas de “tax” comme avec des milliers de requêtes par offre.

---

### E. Couche staging (nettoyage et typage)

1. **staging_geo** : vues ou tables dérivées de `raw_geo_*` (types cohérents, champs renommés si besoin).
2. **staging_offres** : jointure `raw_france_travail_offres` + `offres_avec_siren` + éventuellement geo (commune/dépt/région) pour avoir lieu et entreprise dans une même table.
3. **staging_sirene** : sélection des colonnes utiles de `raw_sirene_unite_legale` et `raw_sirene_etablissement`, typage (dates, nombres).

**Pourquoi** : séparer le brut (raw) du “nettoyé” (staging), comme en local ; les marts ne consomment que du staging.

---

### F. Couche marts (schéma en étoile)

1. **Dimensions** : `dim_geo` (commune/dépt/région), `dim_entreprise` (SIREN + dénomination, NAF), `dim_rome`, `dim_contrat`, `dim_temps` (année, mois, etc.).
2. **Table de fait** : `fact_offres` (id_offre, clés vers dim_geo, dim_entreprise, dim_rome, dim_contrat, dim_temps, salaire ou indicateurs dérivés).
3. Remplir les marts à partir des tables/vues staging (requêtes `INSERT` ou `MERGE`, ou modèles Dataform/dbt).

**Pourquoi** : répondre à la question du brief (“où, quelles entreprises, quels salaires”) avec des agrégations simples et un dashboard branché sur les marts.

---

### G. Maîtrise du flux (ordonnancement)

1. **Ingestion** :
   - **Géo** : planifier un job (Cloud Scheduler + Cloud Function ou Cloud Run) qui exécute le script “download_geo” et pousse les JSON dans GCS, puis déclenche un load BigQuery (ou un second job).
   - **Offres** : idem avec le script offres (OAuth2 France Travail, pagination), fréquence selon le besoin (ex. quotidien).
   - **Sirene** : mise à jour mensuelle ; script qui télécharge les Parquet (ou réutilise un fichier déjà présent), les dépose dans GCS, puis load BigQuery.
2. **Transformations** :
   - Après chaque ingestion (ou une fois par jour), lancer les requêtes SQL de staging puis marts (via BigQuery Scheduled Queries, ou Dataform/dbt, ou un job Cloud Run qui appelle l’API BigQuery).
3. **Ordre recommandé** : Géo → Offres → Sirene pour l’ingestion ; puis staging (geo, offres+match, sirene) ; puis marts. Documenter cet ordre (README ou doc de flux) pour la reproductibilité.

**Pourquoi** : éviter les exécutions dans le désordre (ex. marts avant que les raw soient à jour) et limiter les coûts en ne lançant les gros jobs qu’après ingestion.

---

### H. Secrets et variables

- **France Travail** : `FRANCE_TRAVAIL_CLIENT_ID` et `FRANCE_TRAVAIL_CLIENT_SECRET` ne doivent pas être en clair dans le code. Utiliser **Secret Manager** (GCP) et les injecter dans Cloud Functions / Cloud Run (variables d’environnement).
- **Clés de compte de service** : pour que les jobs (Functions, Run, BigQuery) accèdent à GCS et BigQuery, utiliser un **compte de service** avec rôles minimaux (ex. `roles/bigquery.dataEditor`, `roles/storage.objectAdmin` sur le bucket raw).

**Pourquoi** : conformité et sécurité ; même principe qu’en local avec `.env`, mais géré par le cloud.

---

### I. Coûts et optimisation

- **BigQuery** : facturation au volume de données **scannées** par requête. Réduire les scans en partitionnant les grosses tables (ex. par date d’ingestion), en ne sélectionnant que les colonnes utiles, et en évitant les requêtes “une par offre” (d’où l’importance du match en une seule requête).
- **GCS** : coût de stockage et de sortie réseau ; garder les raw le temps nécessaire, éventuellement archiver ou supprimer les anciens fichiers selon une politique.
- **Cloud Run / Functions** : facturation à l’exécution ; limiter la fréquence des ingestions (ex. 1 fois par jour pour les offres) pour maîtriser le coût.

**Pourquoi** : le brief demande une estimation et une maîtrise des coûts ; cette architecture permet de les piloter (peu de requêtes lourdes, jobs planifiés).

---

### J. Ce qu’on réutilise du repo actuel

| Élément local | Sur GCP / BigQuery |
|---------------|---------------------|
| `scripts/ingestion/download_geo.py` | Adapter la sortie : écrire dans GCS au lieu de `data/raw/geo/`. |
| `scripts/ingestion/download_offres.py` + `scripts/ingestion/france_travail_auth.py` | Idem : sortie vers GCS ; secrets depuis Secret Manager. |
| Fichiers Sirene (Parquet) | Téléchargement manuel ou script → dépôt dans GCS → `bq load`. |
| `scripts/database/load_local_db.py` | Remplacé par des jobs BigQuery Load (GCS → tables raw). |
| `scripts/matching/match_offres_sirene.py` | Remplacé par une requête SQL BigQuery (JOIN + normalisation, une seule passe). |
| Schéma des jointures (`MODELE_JOINTURES.md`) | Même logique : raw_* + offres_avec_siren → staging → marts. |

---

## 5. Résumé du flux (maîtrise de bout en bout)

1. **Ingestion** (planifiée) : Géo → GCS → raw_geo_* ; Offres → GCS → raw_france_travail_offres ; Sirene → GCS → raw_sirene_*.
2. **Match offres–Sirene** : une requête SQL BigQuery qui remplit `offres_avec_siren` (ou une vue) à partir des raw.
3. **Staging** : vues/tables dérivées des raw + offres_avec_siren, nettoyées et typées.
4. **Marts** : schéma en étoile (fact_offres + dimensions) alimenté depuis le staging.
5. **Dashboard** : outil BI connecté aux marts (Looker Studio, Metabase, etc.).

En suivant ce guide, tu reproduis de A à Z sur GCP et BigQuery ce qui a été fait en local, avec un flux clair, automatisé et maîtrisé (coûts, ordre, secrets).

---

## Références (dans ce repo)

- **`docs/SETUP_LOCAL.md`** — Environnement local, dossiers, SQLite, DBeaver.
- **`docs/MODELE_JOINTURES.md`** — PK/FK, tables raw, `offres_avec_siren`, schéma en étoile.
- **`docs/RAPPROCHEMENT_OFFRES_SIRENE.md`** — Pourquoi le match par nom, index SQLite, usage de `scripts/matching/match_offres_sirene.py`.
- **`docs/queries_exploration_jointures.sql`** — Requêtes d’exploration (cardinalités, jointures test).
- **`docs/FRANCE_TRAVAIL_CREDENTIALS.md`** — OAuth2 France Travail (client credentials, cache token).
