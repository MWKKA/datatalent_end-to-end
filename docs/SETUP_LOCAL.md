# Démarrage en local (sans cloud) — DataTalent

Guide pas à pas pour télécharger les données en local, les charger dans une base, et les explorer avec DBeaver pour **comprendre les données et identifier les clés (PK/FK)** qui relient les tables. Aucun coût cloud.

---

## Vue d’ensemble des 3 sources et des clés de jointure

| Source | Contenu | Format / Accès | Clés importantes |
|--------|---------|----------------|------------------|
| **Sirene (INSEE)** | Entreprises et établissements (raison sociale, NAF, adresse, SIREN/SIRET) | Fichiers Parquet (data.gouv.fr), volumineux | **SIREN** (unité légale), **SIRET** (établissement) |
| **France Travail** | Offres d’emploi (poste, lieu, entreprise, salaire, ROME) | API REST (OAuth2) | **SIRET** (ou identifiant entreprise) pour lier à Sirene ; **code commune / département** pour lier à la Géo |
| **API Géo** | Régions, départements, communes (codes INSEE, coordonnées) | API REST (sans auth) | **code** (région, département, commune) pour enrichir lieux |

**Jointures à viser :**
- **Offres ↔ Sirene** : SIRET (ou SIREN) de l’entreprise qui recrute.
- **Offres ↔ Géo** : code commune ou code département du lieu de travail.
- **Sirene ↔ Géo** : code commune de l’adresse de l’établissement (si disponible dans Sirene).

---

## Étape 1 — Environnement Python

1. Ouvrir un terminal à la racine du projet :  
   `cd /home/antoine/datatalent_end-to-end`
2. Vérifier Python 3.11+ :  
   `python --version` ou `python3 --version`
3. Créer et activer un environnement virtuel (au choix) :
   - avec **uv** (déjà utilisé dans le projet) :  
     `uv sync`
   - ou avec **venv** :  
     `python -m venv .venv` puis `source .venv/bin/activate` (Linux/Mac)
4. Installer les dépendances :  
   - avec **uv** : `uv sync` puis lancer les scripts avec `uv run python scripts/...`
   - avec **pip** : `python -m venv .venv` puis `source .venv/bin/activate` et `pip install -e .`

---

## Étape 2 — Structure des dossiers pour les données

Créer les dossiers qui recevront les fichiers (pattern “raw” pour plus tard Medallion) :

```bash
mkdir -p data/raw/sirene
mkdir -p data/raw/geo
mkdir -p data/raw/france_travail
```

- `data/raw/sirene` : fichiers Sirene (Parquet/CSV).
- `data/raw/geo` : exports API Géo (régions, départements, communes).
- `data/raw/france_travail` : réponses API offres (JSON/Parquet).

Le dossier `data/` est à ajouter dans `.gitignore` si ce n’est pas déjà le cas (pour ne pas versionner des gros fichiers).

---

## Étape 3 — Télécharger les données Sirene (data.gouv.fr)

**Tu n’as pas besoin de tous les fichiers.** Sur la page des ressources, ne télécharge que les **2 suivants** (format **Parquet** de préférence) :

| Fichier | Taille (Parquet) | Rôle |
|--------|-------------------|------|
| **StockEtablissement** | ~2 Go | Établissements : SIRET, SIREN, adresse, NAF → jointure avec les offres (SIRET recruteur). |
| **StockUniteLegale** | ~650 Mo | Unités légales : SIREN, raison sociale → noms d’entreprises pour le dashboard. |

**À ne pas télécharger** pour ce projet : StockEtablissementHistorique, StockUniteLegaleHistorique, StockEtablissementLiensSuccession, StockDoublons (historique, doublons et successions ne servent pas pour “où recrute-t-on, quelles entreprises, quels salaires”).

1. Aller sur :  
   https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/
2. Dans “Ressources” (page 1), télécharger :
   - **Sirene : Fichier StockEtablissement du … (format parquet)** → ~2 Go
   - **Sirene : Fichier StockUniteLegale du … (format parquet)** → ~650 Mo
3. Placer les fichiers dans `data/raw/sirene/` (pas besoin de décompresser : le Parquet se lit directement).

Si tu veux alléger encore (disque ou temps de chargement), tu peux ne télécharger que **StockEtablissement** dans un premier temps, puis créer un échantillon ou filtrer par département avec un script Python (pandas + pyarrow).

**À retenir pour les clés :**
- **Etablissement** : `siret` (14 chiffres) = PK ; `siren` (9 chiffres) = FK vers UniteLegale.
- **UniteLegale** : `siren` = PK.
- Relation : un SIREN a plusieurs établissements (plusieurs SIRET).

---

## Étape 4 — Récupérer les référentiels API Géo

L’API Géo ne nécessite pas d’authentification. On peut tout récupérer en quelques appels.

1. Endpoints utiles (JSON) :
   - Régions : `https://geo.api.gouv.fr/regions`
   - Départements : `https://geo.api.gouv.fr/departements`
   - Communes (liste complète plus lourde) : `https://geo.api.gouv.fr/communes` (ou par département si disponible)
2. Avec un script Python (requests) ou avec `curl`, enregistrer les réponses en JSON dans `data/raw/geo/`, par exemple :
   - `regions.json`
   - `departements.json`
   - `communes.json` (ou un échantillon si trop gros)

**Clés utiles :**
- Région : `code` (ex. "32").
- Département : `code` (ex. "75"), `region.code` pour lier à la région.
- Commune : `code` (code INSEE), `departement.code`, `codesPostaux`, etc.

---

## Étape 5 — (Optionnel) Récupérer des offres France Travail

Pour éviter les coûts et la complexité OAuth2 au tout début, tu peux **reporter** cette étape et travailler d’abord avec Sirene + Géo.

Si tu veux déjà des offres en local :

1. Créer un compte partenaire sur **France Travail** (ex. https://francetravail.io) et obtenir des identifiants OAuth2.
2. Placer en local les secrets dans un fichier `.env` (jamais versionné), ex. :
   - `FRANCE_TRAVAIL_CLIENT_ID=...`
   - `FRANCE_TRAVAIL_CLIENT_SECRET=...`
3. Utiliser un script d’ingestion qui :
   - récupère un token OAuth2,
   - appelle l’endpoint des offres (ex. par code ROME “M1805” pour type Data Engineer, par département),
   - pagine si nécessaire,
   - enregistre les réponses en JSON dans `data/raw/france_travail/`.

Dans la réponse API, repérer le champ qui identifie l’**entreprise** (souvent un **SIRET** ou identifiant équivalent) : ce sera la clé de jointure avec la table Sirene (établissement ou unité légale). Repérer aussi le **lieu** (code commune ou département) pour la jointure avec la Géo.

---

## Étape 6 — Base de données locale pour DBeaver

Pour explorer avec DBeaver, il faut une base SQL locale.

**Base sur Windows (DBeaver sous Windows, repo sous WSL)** : place `local.db` côté Windows (ex. `C:\Users\antoi\Downloads\local.db`). Dans ton `.env` (racine du projet, WSL) ajoute :
`DATABASE_PATH=/mnt/c/Users/antoi/Downloads/local.db`
→ les scripts chargeront et liront cette base.

1. **Choisir un moteur** :
   - **SQLite** : un seul fichier, pas de serveur (idéal pour test rapide).
   - **PostgreSQL** (Docker) : plus proche d’un environnement “pro” (BigQuery, Snowflake, etc.).
2. **Créer la base** :
   - SQLite : le script `scripts/database/load_local_db.py` crée la base (par défaut `data/local.db`, ou le chemin de `DATABASE_PATH`).
   - PostgreSQL :  
     `docker run -d --name postgres-datatalent -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=datatalent -p 5432:5432 postgres:16`
3. **Ouvrir DBeaver** :
   - Nouvelle connexion → SQLite (fichier `data/local.db`) ou PostgreSQL (host localhost, port 5432, user/password/DB selon ton run).
   - Tester la connexion.

---

## Étape 7 — Charger les données dans la base

Le script **`scripts/database/load_local_db.py`** crée `data/local.db` et remplit les tables raw (Géo, offres, et Sirene si des Parquet sont présents).

Commandes typiques :

```bash
# Tout avoir dans DBeaver (Géo + Offres + Sirene) : lancer en 3 fois pour limiter la mémoire
uv run python scripts/database/load_local_db.py --geo-only
uv run python scripts/database/load_local_db.py --offres-only
uv run python scripts/database/load_local_db.py --sirene-only
```

(Sirene est chargé par blocs depuis les Parquet ; les offres en une fois — si erreur mémoire, ajouter `--max-offres 500` à la 2e commande.)

```bash
# Variantes
uv run python scripts/database/load_local_db.py                    # tout en une fois (peut manquer de RAM)
uv run python scripts/database/load_local_db.py --offres-only --max-offres 500   # limiter les offres
uv run python scripts/database/load_local_db.py --communes        # inclure les communes Géo (~35k)
```

Tables créées : `raw_geo_regions`, `raw_geo_departements`, `raw_geo_communes` (si `--communes`), `raw_france_travail_offres`, et éventuellement `raw_sirene_etablissement` / `raw_sirene_unite_legale` si des fichiers Parquet Sirene sont dans `data/raw/sirene/`.

---

## Étape 8 — Explorer dans DBeaver et identifier les PK/FK

1. Dans DBeaver, ouvrir les tables et regarder les **premiers enregistrements** (structure et exemples de valeurs).
2. Pour chaque table, **identifier** :
   - la **clé primaire (PK)** : colonne(s) unique(s) — ex. `siret` pour établissement, `siren` pour unité légale, `code` pour région/département/commune.
   - les **clés étrangères (FK)** : colonnes qui font référence à une autre table — ex. `siren` dans `stockEtablissement` → `stockUniteLegale.siren` ; code département dans offres → `departements.code`.
3. Noter dans un fichier (ex. `docs/MODELE_JOINTURES.md`) :
   - nom de la table,
   - PK,
   - FK vers quelle table,
   - comment tu relieras offres ↔ Sirene (SIRET) et offres ↔ Géo (code commune/département).

À ce stade, tu as tout en local, sans cloud, et une vision claire des données et des jointures pour la suite (staging, marts, dashboard).

---

## Résumé des étapes (checklist)

1. [ ] Environnement Python (uv ou venv) et dépendances installées.
2. [ ] Dossiers `data/raw/sirene`, `data/raw/geo`, `data/raw/france_travail` créés.
3. [ ] Données Sirene téléchargées (stock ou extrait) dans `data/raw/sirene`.
4. [ ] Référentiels Géo récupérés (régions, départements, communes) dans `data/raw/geo`.
5. [ ] (Optionnel) Offres France Travail récupérées dans `data/raw/france_travail`.
6. [ ] Base locale créée (SQLite ou PostgreSQL) et connexion DBeaver configurée.
7. [ ] Données chargées en tables (raw) dans la base.
8. [ ] Exploration DBeaver : identification des PK/FK et documentation des jointures.

Une fois cette base locale et ces jointures claires, tu pourras répliquer le même schéma en cloud (Medallion raw → staging → marts) et brancher le dashboard.
