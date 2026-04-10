{{ config(materialized='table', alias='offres_staging') }}

/*
  Détail France Travail extrait du payload_json (API v2) + colonnes déjà présentes sur raw_offres_team.
  id_offre = offer_id FT pour la jointure avec offres_enriched_clean.
*/

SELECT
  o.offer_id AS id_offre,
  COALESCE(JSON_VALUE(o.payload_json, '$.intitule'), o.intitule) AS intitule,
  JSON_VALUE(o.payload_json, '$.description') AS description,
  SAFE_CAST(JSON_VALUE(o.payload_json, '$.dateCreation') AS TIMESTAMP) AS date_creation,
  SAFE_CAST(JSON_VALUE(o.payload_json, '$.dateActualisation') AS TIMESTAMP) AS date_actualisation,
  COALESCE(JSON_VALUE(o.payload_json, '$.lieuTravail.libelle'), o.location_name) AS lieu_libelle,
  SAFE_CAST(JSON_VALUE(o.payload_json, '$.lieuTravail.latitude') AS FLOAT64) AS lieu_latitude,
  SAFE_CAST(JSON_VALUE(o.payload_json, '$.lieuTravail.longitude') AS FLOAT64) AS lieu_longitude,
  JSON_VALUE(o.payload_json, '$.lieuTravail.codePostal') AS lieu_code_postal,
  JSON_VALUE(o.payload_json, '$.lieuTravail.commune') AS lieu_commune,
  COALESCE(JSON_VALUE(o.payload_json, '$.romeCode'), o.rome_code) AS rome_code,
  JSON_VALUE(o.payload_json, '$.romeLibelle') AS rome_libelle,
  JSON_VALUE(o.payload_json, '$.appellationLibelle') AS appellation_libelle,
  JSON_VALUE(o.payload_json, '$.entreprise.nom') AS entreprise_nom,
  JSON_VALUE(o.payload_json, '$.entreprise.description') AS entreprise_description,
  JSON_VALUE(o.payload_json, '$.typeContrat') AS type_contrat,
  JSON_VALUE(o.payload_json, '$.typeContratLibelle') AS type_contrat_libelle,
  COALESCE(
    JSON_VALUE(o.payload_json, '$.natureContrat'),
    JSON_VALUE(o.payload_json, '$.natureContratLibelle')
  ) AS nature_contrat,
  JSON_VALUE(o.payload_json, '$.experienceExige') AS experience_exige,
  JSON_VALUE(o.payload_json, '$.experienceLibelle') AS experience_libelle,
  JSON_VALUE(o.payload_json, '$.dureeTravailLibelle') AS duree_travail_libelle,
  CASE
    WHEN JSON_VALUE(o.payload_json, '$.alternance') IS NULL THEN CAST(NULL AS BOOL)
    WHEN LOWER(TRIM(JSON_VALUE(o.payload_json, '$.alternance'))) IN ('true', '1', 'oui', 'o')
      THEN TRUE
    WHEN SAFE_CAST(JSON_VALUE(o.payload_json, '$.alternance') AS INT64) = 1
      THEN TRUE
    WHEN SAFE_CAST(JSON_VALUE(o.payload_json, '$.alternance') AS INT64) = 0
      THEN FALSE
    ELSE FALSE
  END AS alternance,
  SAFE_CAST(JSON_VALUE(o.payload_json, '$.nombrePostes') AS INT64) AS nombre_postes,
  COALESCE(
    JSON_VALUE(o.payload_json, '$.salaire.libelle'),
    JSON_VALUE(o.payload_json, '$.salaireLibelle')
  ) AS salaire_libelle,
  COALESCE(JSON_VALUE(o.payload_json, '$.codeNAF'), o.code_naf_offre) AS code_naf,
  JSON_VALUE(o.payload_json, '$.secteurActivite') AS secteur_activite,
  JSON_VALUE(o.payload_json, '$.secteurActiviteLibelle') AS secteur_activite_libelle
FROM {{ source('raw', 'raw_offres_team') }} AS o
WHERE o.is_france_travail = 1
