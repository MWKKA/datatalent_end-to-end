{# communes_raw : colonnes STRING (nouveau load) ou typées par autodetect (ancien) — CAST vers STRING avant TRIM. #}

{% macro communes_trim_text(col) -%}
NULLIF(TRIM(CAST({{ col }} AS STRING)), '')
{%- endmacro %}

{% macro communes_trim_int(col) -%}
SAFE_CAST(NULLIF(TRIM(CAST({{ col }} AS STRING)), '') AS INT64)
{%- endmacro %}

{% macro communes_trim_float(col) -%}
SAFE_CAST(NULLIF(TRIM(CAST({{ col }} AS STRING)), '') AS FLOAT64)
{%- endmacro %}
