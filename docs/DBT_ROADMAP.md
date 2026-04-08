# Roadmap dbt (simple et progressive)

Objectif: passer du SQL local actuel a un pipeline dbt propre, testable et documente.

---

## Etape 1 - Initialisation projet dbt (BigQuery)

- Configurer `dbt_project.yml` (actuellement vide)
- Ajouter `profiles.yml.example` avec target BigQuery (sans secrets)
- Definir les schemas:
  - `raw` (sources)
  - `staging`
  - `intermediate`
  - `marts`

---

## Etape 2 - Declarer les sources

- Sources minimales:
  - `raw.offres_raw`
  - `raw.communes_raw`
  - `raw.adzuna_jobs` (si retenu par la team)
  - Sirene raw (a confirmer)

Livrable:
- fichier `sources.yml` documente (description + freshness si possible)

---

## Etape 3 - Migrer les modeles SQL

- `stg_offres` (depuis SQL local)
- `int_offres_entreprises`
- `fact_offres`

Regle:
- 1 modele = 1 responsabilite
- noms explicites
- materialization simple (`view` puis `table` si besoin)

---

## Etape 4 - Ajouter les tests dbt

Tests minimaux requis:
- `not_null` sur `offre_id`
- `unique` sur `offre_id` (staging et/ou fact)
- `relationships`:
  - `fact_offres.commune_code` -> `dim_commune.commune_code`
  - `fact_offres.siren` -> `dim_entreprise.siren` (quand dispo)
- `accepted_values` sur `type_contrat` (liste a confirmer)

---

## Etape 5 - Documentation et lineage

- descriptions des modeles et colonnes critiques
- `dbt docs generate` pour visualiser le lineage
- ajouter captures/lien docs dans README

---

## Definition of Done (dbt)

- `dbt run` passe sans erreur
- `dbt test` passe sans erreur bloquante
- lineage visible
- modele final repond a: "ou recrute-t-on, quelles entreprises, quels salaires ?"

