# BigQuery - Point d'etat et validation equipe

Ce document sert de base de discussion avant toute action sur BigQuery.
Regle appliquee: **aucune requete executee sans validation explicite**.

---

## 1) Contexte

- Projet GCP: `datatalent-simplon`
- Region BigQuery: `EU`
- Source d'information actuelle: inventaire manuel fourni depuis la console GCP
- Contrainte equipe: toute modification BigQuery doit etre documentee ici avant execution

---

## 2) Etat actuel connu (sans execution de requete)

### Dataset `raw`

- `raw.offres_raw`
  - lignes: 364
  - taille logique: 1.53 Mo
- `raw.communes_raw`
  - lignes: 34 934
  - taille logique: 18.87 Mo
- `raw.adzuna_jobs`
  - lignes: 2 209
  - taille logique: 4.61 Mo

### Dataset `staging`

- `staging.offres_staging`
  - lignes: 364
  - taille logique: 1.16 Mo
- `staging.dim_commune`
  - lignes: 34 934
- `staging.dim_departement`
  - lignes: 101
- `staging.dim_region`
  - lignes: 18
- `staging.adzuna_jobs_clean`
  - lignes: 888

---

## 3) Lecture rapide vs brief

### Ce qui est coherent

- `raw` et `staging` existent deja.
- La dimension geo parait propre (18 regions, 101 departements, 34 934 communes).
- Adzuna est present (valide par le formateur), donc peut etre garde comme source complementaire.

### Ce qui semble incomplet a ce stade

- Tables Sirene (`raw_sirene_unite_legale` / `raw_sirene_etablissement`) non confirmees.
- Couche `intermediate` explicite non visible (matching offres -> entreprise).
- Couche `marts` non visible (ex: `fact_offres`).

---

## 4) Requetes proposees (a valider AVANT execution)

Objectif: verifier l'existant a cout faible, lecture seule.

> Important: ne rien executer tant que la team n'a pas valide ce bloc.

### Q1 - Inventaire exact des tables

```sql
SELECT table_schema, table_name, table_type
FROM `datatalent-simplon`.raw.INFORMATION_SCHEMA.TABLES
UNION ALL
SELECT table_schema, table_name, table_type
FROM `datatalent-simplon`.staging.INFORMATION_SCHEMA.TABLES
ORDER BY table_schema, table_name;
```

### Q2 - Schema colonnes des tables critiques

```sql
SELECT table_name, column_name, data_type
FROM `datatalent-simplon`.raw.INFORMATION_SCHEMA.COLUMNS
WHERE table_name IN ('offres_raw', 'communes_raw', 'adzuna_jobs')
ORDER BY table_name, ordinal_position;
```

### Q3 - Controles qualite de base sur `staging.offres_staging`

```sql
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(offre_id IS NULL) AS null_offre_id,
  COUNT(DISTINCT offre_id) AS distinct_offre_id
FROM `datatalent-simplon.staging.offres_staging`;
```

### Q4 - Couverture geo des offres staging

```sql
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(commune_code IS NOT NULL AND commune_code != '') AS offers_with_commune_code
FROM `datatalent-simplon.staging.offres_staging`;
```

---

## 5) Decision team attendue

- [ ] Valider l'execution de Q1
- [ ] Valider l'execution de Q2
- [ ] Valider l'execution de Q3
- [ ] Valider l'execution de Q4
- [ ] Confirmer les datasets cibles pour la suite (`raw`, `staging`, `intermediate`, `marts`)
- [ ] Confirmer la place d'Adzuna dans le modele final (source complementaire ou scope secondaire)

---

## 6) Suite proposee apres validation

1. Faire le point exact sur schemas/tables BigQuery existantes.
2. Aligner le modele local SQL (`staging/intermediate/marts`) avec ce qui existe sur BQ.
3. Industrialiser avec dbt (requis probable): models, tests, docs, lineage.
4. Ne publier sur BQ que des changements valides par l'equipe.

