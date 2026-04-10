{{ config(materialized='table') }}

/*
  Source : raw.communes_raw — schéma variable :
  - nouveau pipeline : colonnes STRING ;
  - ancien autodetect : INT64/FLOAT64 sur les champs numériques.

  CAST(... AS STRING) avant TRIM évite l’erreur « TRIM(FLOAT64) ».
*/

SELECT
  {{ communes_trim_text('code_insee') }} AS code_insee,
  {{ communes_trim_text('nom_standard') }} AS nom_standard,
  {{ communes_trim_text('nom_sans_pronom') }} AS nom_sans_pronom,
  {{ communes_trim_text('nom_a') }} AS nom_a,
  {{ communes_trim_text('nom_de') }} AS nom_de,
  {{ communes_trim_text('nom_sans_accent') }} AS nom_sans_accent,
  {{ communes_trim_text('nom_standard_majuscule') }} AS nom_standard_majuscule,

  {{ communes_trim_text('typecom') }} AS typecom,
  {{ communes_trim_text('typecom_texte') }} AS typecom_texte,

  {{ communes_trim_text('reg_code') }} AS reg_code,
  {{ communes_trim_text('reg_nom') }} AS reg_nom,
  {{ communes_trim_text('dep_code') }} AS dep_code,
  {{ communes_trim_text('dep_nom') }} AS dep_nom,
  {{ communes_trim_text('canton_code') }} AS canton_code,
  {{ communes_trim_text('canton_nom') }} AS canton_nom,
  {{ communes_trim_text('epci_code') }} AS epci_code,
  {{ communes_trim_text('epci_nom') }} AS epci_nom,
  {{ communes_trim_text('academie_code') }} AS academie_code,
  {{ communes_trim_text('academie_nom') }} AS academie_nom,

  {{ communes_trim_text('code_postal') }} AS code_postal,
  {{ communes_trim_text('codes_postaux') }} AS codes_postaux,
  {{ communes_trim_text('zone_emploi') }} AS zone_emploi,
  {{ communes_trim_text('code_insee_centre_zone_emploi') }} AS code_insee_centre_zone_emploi,
  {{ communes_trim_text('code_unite_urbaine') }} AS code_unite_urbaine,
  {{ communes_trim_text('nom_unite_urbaine') }} AS nom_unite_urbaine,
  {{ communes_trim_text('taille_unite_urbaine') }} AS taille_unite_urbaine,
  {{ communes_trim_text('type_commune_unite_urbaine') }} AS type_commune_unite_urbaine,
  {{ communes_trim_text('statut_commune_unite_urbaine') }} AS statut_commune_unite_urbaine,

  {{ communes_trim_int('population') }} AS population,
  {{ communes_trim_float('superficie_hectare') }} AS superficie_hectare,
  {{ communes_trim_float('superficie_km2') }} AS superficie_km2,
  {{ communes_trim_float('densite') }} AS densite,
  {{ communes_trim_float('altitude_moyenne') }} AS altitude_moyenne,
  {{ communes_trim_float('altitude_minimale') }} AS altitude_minimale,
  {{ communes_trim_float('altitude_maximale') }} AS altitude_maximale,

  {{ communes_trim_float('latitude_mairie') }} AS latitude_mairie,
  {{ communes_trim_float('longitude_mairie') }} AS longitude_mairie,
  {{ communes_trim_float('latitude_centre') }} AS latitude_centre,
  {{ communes_trim_float('longitude_centre') }} AS longitude_centre,

  {{ communes_trim_int('grille_densite') }} AS grille_densite,
  {{ communes_trim_text('grille_densite_texte') }} AS grille_densite_texte,
  {{ communes_trim_int('niveau_equipements_services') }} AS niveau_equipements_services,
  {{ communes_trim_text('niveau_equipements_services_texte') }} AS niveau_equipements_services_texte,

  {{ communes_trim_text('gentile') }} AS gentile,
  {{ communes_trim_text('url_wikipedia') }} AS url_wikipedia,
  {{ communes_trim_text('url_villedereve') }} AS url_villedereve

FROM {{ source('raw', 'communes_raw') }}
