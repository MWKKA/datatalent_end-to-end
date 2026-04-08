# Rapprochement offres France Travail ↔ Sirene (trouver le SIREN)

L’API France Travail ne renvoie **pas** de SIRET ni SIREN dans l’objet `entreprise`, seulement `nom`. Pour lier les offres aux unités légales Sirene, on fait un **rapprochement par nom** (et optionnellement par code NAF).

**Base SQLite sur Windows (DBeaver)** : si ta base est dans `C:\Users\antoi\Downloads\local.db`, ajoute dans ton `.env` à la racine du projet (côté WSL) :
```bash
DATABASE_PATH=/mnt/c/Users/antoi/Downloads/local.db
```
Tous les scripts (`scripts/database/load_local_db.py`, `scripts/matching/match_offres_sirene.py`) liront et écriront alors dans ce fichier.

---

## Pourquoi le script peut sembler bloqué (base Windows)

Le script utilise bien la base indiquée par `DATABASE_PATH` (ex. celle sous Windows). La lenteur vient des requêtes sur **29 M de lignes** dans `raw_sirene_unite_legale` :

- **Match exact** : `WHERE LOWER(TRIM(denominationUniteLegale)) = ?` → l’index sur la colonne brute ne sert pas (à cause de `LOWER`/`TRIM`), SQLite parcourt énormément de lignes.
- **Match par préfixe** (si pas d’exact) : `WHERE LOWER(TRIM(...)) LIKE 'jcdecaux%'` → idem, parcours très coûteux.

Résultat : chaque offre qui passe par le fallback LIKE peut prendre **plusieurs minutes**. Tant qu’on n’a pas traité 10 offres, aucun message n’apparaît → on a l’impression que ça ne marche pas.

**Solution (recommandé)** : créer un **index sur l’expression** dans DBeaver (sur la même base que celle utilisée par le script), **avant** de relancer le script. Création longue une fois (plusieurs minutes), puis le script devient rapide :

```sql
CREATE INDEX IF NOT EXISTS idx_sirene_ul_denom_lower
ON raw_sirene_unite_legale(LOWER(TRIM(denominationUniteLegale)));
```

Ensuite relancer `uv run python scripts/matching/match_offres_sirene.py` : les requêtes utiliseront cet index et le script devrait terminer en quelques dizaines de secondes.

---

## Étape 1 — Créer un index sur le nom en Sirene (une fois)

Sans index, une recherche par `denominationUniteLegale` sur 29 M de lignes est très lente. À exécuter **une seule fois** dans DBeaver (peut prendre plusieurs minutes) :

```sql
CREATE INDEX IF NOT EXISTS idx_sirene_ul_denomination
ON raw_sirene_unite_legale(denominationUniteLegale);
```

Optionnel : pour affiner avec le NAF plus tard, tu peux aussi créer :

```sql
CREATE INDEX IF NOT EXISTS idx_sirene_ul_activite
ON raw_sirene_unite_legale(activitePrincipaleUniteLegale);
```

---

## Étape 2 — Lancer le script de rapprochement

Le script Python lit les offres dans la base, cherche pour chaque nom d’entreprise un SIREN dans Sirene (match exact sur la dénomination), et enregistre le résultat dans une table.

```bash
uv run python scripts/matching/match_offres_sirene.py
```

Il crée la table **`offres_avec_siren`** (ou met à jour) avec : `offre_id`, `entreprise_nom`, `siren`, `denomination_sirene`, `match_naf` (vrai si le code NAF de l’offre correspond à celui de l’unité légale).

---

## Étape 3 — Vérifier les résultats dans DBeaver

```sql
-- Nombre d’offres avec au moins un SIREN trouvé
SELECT COUNT(DISTINCT offre_id) AS nb_offres_matchées FROM offres_avec_siren WHERE siren IS NOT NULL;

-- Exemples de matchs
SELECT * FROM offres_avec_siren WHERE siren IS NOT NULL LIMIT 20;

-- Offres sans match (à traiter plus tard : nom différent, typo, etc.)
SELECT o.id, o.entreprise_nom, o.code_naf
FROM raw_france_travail_offres o
LEFT JOIN offres_avec_siren m ON m.offre_id = o.id AND m.siren IS NOT NULL
WHERE m.offre_id IS NULL;
```

---

## Règles de matching utilisées par le script

1. **Normalisation** : comparaison en minuscules, sans espaces en trop (`LOWER(TRIM(nom))`).
2. **Match exact** : `entreprise_nom` (offres) = `denominationUniteLegale` (Sirene).
3. **Plusieurs SIREN pour un même nom** : le script garde le premier trouvé ; si tu veux affiner, tu peux utiliser le **code NAF** (offres.`code_naf` = Sirene.`activitePrincipaleUniteLegale`) pour départager ou valider.

Tu peux ensuite enrichir le script (match flou, suppression des formes juridiques "SAS", "SA", etc.) si besoin.
