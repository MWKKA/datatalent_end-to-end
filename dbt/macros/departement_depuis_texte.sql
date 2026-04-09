{# Déduit un département à partir d'un texte libre (ville/libellé/commune). #}
{% macro departement_depuis_texte(col) %}
CASE
  WHEN {{ col }} IS NULL THEN NULL
  WHEN REGEXP_CONTAINS(
    REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(TRIM({{ col }}), NFD), r'\pM', ''),
    r'(^|[^a-z])nord([^a-z]|$)'
  ) THEN '59'
  WHEN REGEXP_CONTAINS(
    REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(TRIM({{ col }}), NFD), r'\pM', ''),
    r'(^|[^a-z])pas[- ]de[- ]calais([^a-z]|$)'
  ) THEN '62'
  WHEN REGEXP_CONTAINS(
    REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(TRIM({{ col }}), NFD), r'\pM', ''),
    r'(^|[^a-z])loire[- ]atlantique([^a-z]|$)'
  ) THEN '44'
  WHEN REGEXP_CONTAINS(
    REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(TRIM({{ col }}), NFD), r'\pM', ''),
    r'(^|[^a-z])gironde([^a-z]|$)'
  ) THEN '33'
  WHEN REGEXP_CONTAINS(
    REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(TRIM({{ col }}), NFD), r'\pM', ''),
    r'(^|[^a-z])rhone([^a-z]|$)'
  ) THEN '69'
  WHEN REGEXP_CONTAINS(
    REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(TRIM({{ col }}), NFD), r'\pM', ''),
    r'(^|[^a-z])haute[- ]garonne([^a-z]|$)'
  ) THEN '31'
  WHEN REGEXP_CONTAINS(
    REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(TRIM({{ col }}), NFD), r'\pM', ''),
    r'(^|[^a-z])ille[- ]et[- ]vilaine([^a-z]|$)'
  ) THEN '35'
  WHEN REGEXP_CONTAINS(
    REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(TRIM({{ col }}), NFD), r'\pM', ''),
    r'(^|[^a-z])bas[- ]rhin([^a-z]|$)'
  ) THEN '67'
  WHEN REGEXP_CONTAINS(
    REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(TRIM({{ col }}), NFD), r'\pM', ''),
    r'(^|[^a-z])hauts[- ]de[- ]seine([^a-z]|$)'
  ) THEN '92'
  WHEN REGEXP_CONTAINS(
    REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(TRIM({{ col }}), NFD), r'\pM', ''),
    r'(^|[^a-z])seine[- ]et[- ]marne([^a-z]|$)'
  ) THEN '77'
  WHEN REGEXP_CONTAINS(
    REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(TRIM({{ col }}), NFD), r'\pM', ''),
    r'(^|[^a-z])yvelines([^a-z]|$)'
  ) THEN '78'
  WHEN REGEXP_CONTAINS(
    REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(TRIM({{ col }}), NFD), r'\pM', ''),
    r'(^|[^a-z])essonne([^a-z]|$)'
  ) THEN '91'
  WHEN REGEXP_CONTAINS(
    REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(TRIM({{ col }}), NFD), r'\pM', ''),
    r'(^|[^a-z])seine[- ]saint[- ]denis([^a-z]|$)'
  ) THEN '93'
  WHEN REGEXP_CONTAINS(
    REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(TRIM({{ col }}), NFD), r'\pM', ''),
    r'(^|[^a-z])val[- ]de[- ]marne([^a-z]|$)'
  ) THEN '94'
  WHEN REGEXP_CONTAINS(
    REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(TRIM({{ col }}), NFD), r'\pM', ''),
    r'(^|[^a-z])val[- ]d[- ]oise([^a-z]|$)'
  ) THEN '95'
  WHEN REGEXP_CONTAINS(
    REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(TRIM({{ col }}), NFD), r'\pM', ''),
    r'(^|[^a-z])paris([^a-z]|$)'
  ) THEN '75'
  ELSE NULL
END
{% endmacro %}
