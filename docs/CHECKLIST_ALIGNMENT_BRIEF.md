# Checklist d'alignement au brief

Etat actuel du projet par rapport aux consignes officielles.  
Legende:
- `[x]` Fait
- `[~]` Partiel
- `[ ]` Pas encore fait

---

## 1) Cadrage / cartographie (Phase 1)

- `[x]` Les 3 sources sont identifiees et documentees (`brief.md`, `docs/SETUP_LOCAL.md`).
- `[x]` Les champs de jointure sont identifies (avec limite cle cote offres) (`docs/MODELE_JOINTURES.md`).
- `[x]` Les limites qualite sont explicitees (absence SIRET cote offres, matching par nom).
- `[~]` Choix cloud deploiement effectif (doc GCP faite, infra non deployee).
- `[ ]` Kanban public (Trello ou equivalent) avec user stories/sprints et historique.

Tests / verifications:
- Verifier que le lien Kanban sera ajoute dans `README.md` avant soutenance.
- Verifier que toute action BigQuery est documentee dans `docs/BIGQUERY_TEAM_VALIDATION.md` avant execution.

Hypotheses:
- Le projet continue sur GCP + BigQuery (doc deja redigee).

Validation attendue:
- Relecteur peut retrouver rapidement "quoi/comment/pourquoi" des 3 sources et jointures.

---

## 2) Automatisation extraction (Phase 2)

- `[x]` Script Géo fonctionnel (`scripts/ingestion/download_geo.py`).
- `[x]` Auth OAuth2 France Travail avec cache token (`scripts/ingestion/france_travail_auth.py`).
- `[x]` Script Offres fonctionnel (`scripts/ingestion/download_offres.py`).
- `[x]` Chargement Sirene local (Parquet) via `scripts/database/load_local_db.py`.
- `[~]` Gestion des erreurs presente mais perfectible (retries structurés, backoff centralise).
- `[~]` Journalisation: presente via `print`, pas encore logs structures (niveau/horodatage).
- `[~]` Idempotence: partielle (re-ecriture fichiers/tables), pas de strategy d'historisation explicite.
- `[ ]` Pagination nationale par 101 departements (aujourd'hui mots-cles globaux).
- `[ ]` Planification automatique (ordonnanceur + execution conteneurisee).

Tests / verifications:
- Tester un run complet "raw local" de bout en bout:
  1) `scripts/ingestion/download_geo.py`
  2) `scripts/ingestion/download_offres.py`
  3) `scripts/database/load_local_db.py --geo-only --offres-only --sirene-only`
  4) `scripts/matching/match_offres_sirene.py`

Hypotheses:
- L'API offres continue d'etre plus fiable en `motsCles` qu'en filtre ROME strict.

Validation attendue:
- Le run complet doit etre reproductible sans action manuelle dans SQL.

---

## 3) Transformations SQL (Phase 3)

- `[x]` Requetes d'exploration et preparation des jointures (`docs/queries_exploration_jointures.sql`).
- `[x]` Table intermediaire de matching `offres_avec_siren` (logique intermediate locale).
- `[~]` Medallion applique en local surtout sur `raw`; `staging/marts` a completer functionnellement.
- `[~]` Modeles SQL versionnes en dossiers `staging / intermediate / marts` (structure creee, contenu a finaliser).
- `[ ]` Tests qualite de donnees (not_null, unique, accepted_values) automatises.
- `[ ]` Lignage outille (dbt docs, Data Catalog, ou equivalent).

Tests / verifications:
- Definir une batterie SQL minimale:
  - unicite `raw_france_travail_offres.id`
  - non null `offres_avec_siren.offre_id`
  - integrite referentielle `offres_avec_siren.offre_id -> raw_france_travail_offres.id`

Hypotheses:
- La couche `intermediate` gardera le matching offres-Sirene.

Validation attendue:
- Un run SQL doit produire des tables analytiques lisibles sans retraitement manuel.

---

## 4) Infra, couts, CI/CD (Phase 4)

- `[~]` Squelette infra present (`terraform/`) mais fichiers vides.
- `[~]` Squelette CI/CD present (`.github/workflows/`) mais workflows vides.
- `[~]` Dockerfile / docker-compose presents mais vides.
- `[~]` Trajectoire dbt formalisee (`docs/DBT_ROADMAP.md`), implementation technique a faire.
- `[ ]` IaC complet: bucket, entrepot, IAM, scheduler, secrets, service serverless.
- `[ ]` CI/CD effectif: lint Python + build SQL + validation IaC + deploy main.
- `[ ]` Estimation couts documentee (Infracost ou estimateur GCP).
- `[ ]` Optimisation couts implementee (partitionnement/clustering/planification).

Tests / verifications:
- Sur chaque PR: lint + tests + verification IaC.
- Sur `main`: deploy auto + run smoke test.

Hypotheses:
- Terraform sera l'outil IaC principal.

Validation attendue:
- Environnement recreable uniquement via IaC (pas de ressources manuelles).

---

## 5) Dashboard, gouvernance, catalogue (Phase 5)

- `[ ]` Dashboard analytique public (angles geo, sectoriel, temporel).
- `[ ]` Dashboard de couts cloud (service, evolution, alertes budget).
- `[ ]` Catalogue de donnees (description, owner, frequence, tags sensibilite).
- `[ ]` Schema d'architecture image/draw.io du flux complet.

Tests / verifications:
- Verifier que les liens publics dashboard + kanban sont dans `README.md`.
- Verifier qu'un reviewer peut refaire la demo en 15 min max.

Hypotheses:
- Outil BI: Looker Studio (ou equivalent libre).

Validation attendue:
- Le dashboard doit repondre directement a la question centrale du brief.

---

## 6) Livrables repo (etat instantane)

- `[x]` Scripts Python d'ingestion presentes.
- `[~]` `.env.example` present, `requirements.txt` a maintenir aligne avec deps.
- `[~]` Modeles SQL medallion initialises (structure + placeholders), completion fonctionnelle restante.
- `[~]` Modules IaC presents mais vides.
- `[~]` Workflows CI/CD presents mais vides.
- `[~]` Dockerfile / docker-compose presents mais vides.
- `[~]` README present mais encore minimal.
- `[ ]` Lien dashboard analytique public dans README.
- `[ ]` Lien dashboard couts cloud dans README.
- `[ ]` Lien Kanban public dans README.

---

## 7) Decision "raccord consignes" (maintenant)

Conclusion:
- Le projet est **raccord sur la phase exploration locale** et la logique de jointure.
- Le projet est **partiellement raccord** sur industrialisation.
- Les blocs les plus critiques a traiter ensuite:
  1) module matching robuste + qualite,
  2) SQL `staging/intermediate/marts` testes,
  3) IaC + CI/CD + container,
  4) dashboard + gouvernance + couts.




Commandes à utiliser maintenant
uv run python scripts/ingestion/download_geo.py
uv run python scripts/ingestion/download_offres.py
uv run python scripts/database/load_local_db.py --geo-only
uv run python scripts/database/load_local_db.py --offres-only
uv run python scripts/database/load_local_db.py --sirene-only
uv run python scripts/matching/match_offres_sirene.py


Ordre d’exécution (DBeaver / SQLite)
exécuter sql/staging/stg_offres.sql
exécuter sql/intermediate/int_offres_entreprises.sql
exécuter sql/marts/fact_offres.sql
Puis tester :

SELECT COUNT(*) FROM stg_offres;
SELECT COUNT(*) FROM int_offres_entreprises;
SELECT COUNT(*) FROM fact_offres;
SELECT * FROM fact_offres LIMIT 20;