# BigQuery SQL Batch #1 (review equipe)

Statut: **propose - non execute**  
Regle: **aucune execution sans accord explicite equipe**

Projet: `datatalent-simplon`  
Region: `EU`

---

## Objectif du batch

Valider l'existant BigQuery en lecture seule, avec requetes courtes et risque cout faible:
- inventaire tables,
- schema colonnes critiques,
- qualite basique `offres_staging`,
- couverture geo des offres.

---

## Requete 1 - Inventaire tables

**But**  
Lister toutes les tables/vues dans `raw` et `staging`.

**Risque cout**  
Faible (metadata `INFORMATION_SCHEMA`).

**SQL**
```sql
SELECT table_schema, table_name, table_type
FROM `datatalent-simplon.raw.INFORMATION_SCHEMA.TABLES`
UNION ALL
SELECT table_schema, table_name, table_type
FROM `datatalent-simplon.staging.INFORMATION_SCHEMA.TABLES`
ORDER BY table_schema, table_name;
```
**Output observe**
```text
1  raw      adzuna_jobs       BASE TABLE
2  raw      communes_raw      BASE TABLE
3  raw      offres_raw        BASE TABLE
4  staging  adzuna_jobs_clean BASE TABLE
5  staging  dim_commune       BASE TABLE
6  staging  dim_departement   BASE TABLE
7  staging  dim_region        BASE TABLE
8  staging  offres_staging    BASE TABLE
```
**Resultat attendu**
- confirmation des tables reelles disponibles,
- verification presence/absence de Sirene/intermediate/marts.

**Decision equipe**
- [ ] GO
- [ ] NO GO

---

## Requete 2 - Schema colonnes tables critiques raw

**But**  
Verifier les colonnes disponibles pour aligner les modeles SQL/dbt.

**Risque cout**  
Faible (metadata `INFORMATION_SCHEMA`).

**SQL**
```sql
SELECT table_name, column_name, data_type
FROM `datatalent-simplon.raw.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name IN ('offres_raw', 'communes_raw', 'adzuna_jobs')
ORDER BY table_name, ordinal_position;
```
**Output observe (extrait)**
```text
1  adzuna_jobs  ingestion_timestamp   TIMESTAMP
2  adzuna_jobs  raw_json              STRING
3  adzuna_jobs  search_term           STRING
4  adzuna_jobs  location_area         ARRAY<STRING>
5  adzuna_jobs  location_display_name STRING
6  adzuna_jobs  company_name          STRING
7  adzuna_jobs  latitude              FLOAT64
8  adzuna_jobs  job_id                INT64
9  adzuna_jobs  salary_is_predicted   INT64
10 adzuna_jobs  salary_max            INT64
11 adzuna_jobs  longitude             FLOAT64
12 adzuna_jobs  salary_min            INT64
13 adzuna_jobs  redirect_url          STRING
14 adzuna_jobs  description           STRING
15 adzuna_jobs  created               TIMESTAMP
16 adzuna_jobs  collected_at          TIMESTAMP
```
Note: sortie partielle sur `adzuna_jobs` uniquement. Reexecuter Q2 pour `offres_raw` et `communes_raw`.
**Resultat attendu**
- mapping clair des colonnes,
- identification des champs de jointure utilisables.

**Decision equipe**
- [ ] GO
- [ ] NO GO

---

## Requete 3 - Qualite minimale `staging.offres_staging`

**But**  
Verifier volume, nulls et unicite de l'identifiant offre.

**Risque cout**  
Faible (table petite ~364 lignes).

**SQL**
```sql
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(id_offre IS NULL) AS null_id_offre,
  COUNT(DISTINCT id_offre) AS distinct_id_offre
FROM `datatalent-simplon.staging.offres_staging`;
```
**Output observe**
```text
total_rows: 364
null_id_offre: 0
distinct_id_offre: 364
```
**Resultat attendu**
- `null_offre_id = 0`
- `total_rows = distinct_offre_id` (pas de doublon d'id)

**Decision equipe**
- [ ] GO
- [ ] NO GO

---

## Requete 4 - Couverture geo des offres staging

**But**  
Mesurer le taux d'offres avec `commune_code` renseigne.

**Risque cout**  
Faible (table petite ~364 lignes).

**SQL**
```sql
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(lieu_code_postal IS NOT NULL AND lieu_code_postal != '') AS offers_with_lieu_code_postal
FROM `datatalent-simplon.staging.offres_staging`;
```
**Output observe**
```text
offers_with_lieu_code_postal: 298
```
Note: `total_rows` non capture dans la sortie collee. A recapturer pour calculer le taux.

**Resultat attendu**
- indicateur de base pour la future jointure geo dans `fact_offres`.

**Decision equipe**
- [ ] GO
- [ ] NO GO

---

## Validation finale du batch

- [ ] Batch #1 valide pour execution
- [ ] Batch #1 refuse / a modifier
- [ ] Commentaires equipe integres

Prochaine etape si valide:
- execution des requetes 1 a 4,
- stockage des resultats dans un compte-rendu `docs/BIGQUERY_AUDIT_BATCH_01_RESULTS.md`,
- puis preparation du Batch #2 (Sirene + intermediate + marts).

---

## Batch 01 bis (complement indispensable)

Statut: **propose - non execute**

Objectif: completer les sorties manquantes pour pouvoir prendre une decision equipe fiable.

### Q5 - Colonnes `offres_raw` + `communes_raw` (Q2 complet)

**But**  
Completer l'inventaire des colonnes qui manque dans la sortie actuelle.

**Risque cout**  
Faible (metadata `INFORMATION_SCHEMA`).

**SQL**
```sql
SELECT table_name, column_name, data_type
FROM `datatalent-simplon.raw.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name IN ('offres_raw', 'communes_raw')
ORDER BY table_name, ordinal_position;
```


**Output observe**
```text
1	communes_raw	index	INT64
2	communes_raw	code_insee	STRING
3	communes_raw	nom_standard	STRING
4	communes_raw	nom_sans_pronom	STRING
5	communes_raw	nom_a	STRING
6	communes_raw	nom_de	STRING
7	communes_raw	nom_sans_accent	STRING
8	communes_raw	nom_standard_majuscule	STRING
9	communes_raw	typecom	STRING
10	communes_raw	typecom_texte	STRING
11	communes_raw	reg_code	STRING
12	communes_raw	reg_nom	STRING
13	communes_raw	dep_code	STRING
14	communes_raw	dep_nom	STRING
15	communes_raw	canton_code	STRING
16	communes_raw	canton_nom	STRING
17	communes_raw	epci_code	STRING
18	communes_raw	epci_nom	STRING
19	communes_raw	academie_code	STRING
20	communes_raw	academie_nom	STRING
21	communes_raw	code_postal	STRING
22	communes_raw	codes_postaux	STRING
23	communes_raw	zone_emploi	STRING
24	communes_raw	code_insee_centre_zone_emploi	STRING
25	communes_raw	code_unite_urbaine	STRING
26	communes_raw	nom_unite_urbaine	STRING
27	communes_raw	taille_unite_urbaine	FLOAT64
28	communes_raw	type_commune_unite_urbaine	STRING
29	communes_raw	statut_commune_unite_urbaine	STRING
30	communes_raw	population	FLOAT64
31	communes_raw	superficie_hectare	FLOAT64
32	communes_raw	superficie_km2	FLOAT64
33	communes_raw	densite	FLOAT64
34	communes_raw	altitude_moyenne	FLOAT64
35	communes_raw	altitude_minimale	FLOAT64
36	communes_raw	altitude_maximale	FLOAT64
37	communes_raw	latitude_mairie	FLOAT64
38	communes_raw	longitude_mairie	FLOAT64
39	communes_raw	latitude_centre	FLOAT64
40	communes_raw	longitude_centre	FLOAT64
41	communes_raw	grille_densite	FLOAT64
42	communes_raw	grille_densite_texte	STRING
43	communes_raw	niveau_equipements_services	FLOAT64
44	communes_raw	niveau_equipements_services_texte	STRING
45	communes_raw	gentile	STRING
46	communes_raw	url_wikipedia	STRING
47	communes_raw	url_villedereve	STRING
48	offres_raw	raw_json	STRING
49	offres_raw	ingestion_timestamp	TIMESTAMP
50	offres_raw	source_file	STRING```

**Resultat attendu**
- schema complet de `offres_raw` et `communes_raw`,
- validation des champs de jointure utilisables (`id`, `commune_code`, etc.).

**Decision equipe**
- [ ] GO
- [ ] NO GO

---

### Q6 - Couverture geo complete dans `staging.offres_staging`

**But**  
Completer Q4 avec le total + taux pour prise de decision.

**Risque cout**  
Faible (table petite).

**SQL**
```sql
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(lieu_code_postal IS NOT NULL AND lieu_code_postal != '') AS offers_with_lieu_code_postal,
  ROUND(
    100 * COUNTIF(lieu_code_postal IS NOT NULL AND lieu_code_postal != '') / COUNT(*),
    2
  ) AS pct_with_lieu_code_postal
FROM `datatalent-simplon.staging.offres_staging`;
```

**Output observe**
```text

1	364	298	81.87

```

**Resultat attendu**
- taux explicite de couverture geo,
- base de priorisation pour `fact_offres` (jointures geo).

**Decision equipe**
- [ ] GO
- [ ] NO GO

