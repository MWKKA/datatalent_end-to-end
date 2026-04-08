# Modèle des jointures (PK / FK)

Document des jointures validées. Tables dans la base SQLite (`data/local.db` ou chemin `DATABASE_PATH` dans `.env`) après `scripts/database/load_local_db.py` et `scripts/matching/match_offres_sirene.py`.

## Sirene (INSEE)

| Table (DBeaver) | Clé primaire (PK) | Clés étrangères (FK) |
|-----------------|-------------------|----------------------|
| **raw_sirene_etablissement** | `siret` (14 chiffres) | `siren` → raw_sirene_unite_legale |
| **raw_sirene_unite_legale** | `siren` (9 chiffres) | — |

Relation : un établissement (SIRET) appartient à une unité légale (SIREN). Une unité légale peut avoir plusieurs établissements.

## API Géo

| Table (DBeaver) | Clé primaire (PK) | Clés étrangères (FK) |
|-----------------|-------------------|----------------------|
| **raw_geo_regions** | `code` | — |
| **raw_geo_departements** | `code` | `code_region` → raw_geo_regions |
| **raw_geo_communes** | `code` (code INSEE) | `code_departement`, `code_region` |

## France Travail (offres)

| Table (DBeaver) | Clé primaire (PK) | Clés étrangères (FK) |
|-----------------|-------------------|----------------------|
| **raw_france_travail_offres** | `id` | `commune_code` → raw_geo_communes |
| **offres_avec_siren** | `offre_id` | `offre_id` → raw_france_travail_offres.id ; `siren` → raw_sirene_unite_legale.siren |

- **Offres ↔ Géo** : `raw_france_travail_offres.commune_code` (code INSEE commune) → `raw_geo_communes.code` ; sinon `code_postal` pour approche département.
- **Offres ↔ Sirene** : via la table **offres_avec_siren** (remplie par `scripts/matching/match_offres_sirene.py`). L’API offres ne renvoie pas de SIRET/SIREN, donc rapprochement par nom d’entreprise (exact + préfixe normalisé). Colonnes utiles : `offre_id`, `entreprise_nom`, `siren`, `denomination_sirene`, `match_naf` (1 si code NAF offre = NAF Sirene). Taux observé : ~66/150 offres avec un SIREN trouvé sur un échantillon de 150.

---

## Schéma en étoile (cible)

- **Table de fait** : offres (une ligne = une offre), avec clés vers les dimensions.
- **Dimensions** : geo (commune/dépt/région), entreprise (via `offres_avec_siren.siren` → Sirene), ROME, type_contrat, temps (date_creation).
- **Jointure offres → entreprise** : `raw_france_travail_offres.id` = `offres_avec_siren.offre_id` et `offres_avec_siren.siren` = `raw_sirene_unite_legale.siren`.

---

## Requêtes d’exploration

Le fichier **`docs/queries_exploration_jointures.sql`** contient les requêtes pour :
- lister les colonnes (PRAGMA table_info) ;
- vérifier les cardinalités et l’unicité des PK ;
- tester les liens offres → Géo (commune_code, code_postal) ;
- tester le lien Sirene établissement ↔ unité légale (siren) ;
- préparer la jointure offres + geo pour le schéma en étoile ;
- lister les valeurs distinctes (type_contrat, rome) pour les dimensions.
