{% macro normalize_job_title_sql(expr) %}
TRIM(
  REGEXP_REPLACE(
    REPLACE(LOWER(COALESCE({{ expr }}, '')), CHR(160), ' '),
    r'\s+',
    ' '
  )
)
{% endmacro %}
