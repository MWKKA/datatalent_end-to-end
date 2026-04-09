{% snapshot snp_offres_all_sources_scd2 %}

{{
  config(
    target_schema='intermediate',
    unique_key='offer_id',
    strategy='timestamp',
    updated_at='update_date',
    invalidate_hard_deletes=True
  )
}}

SELECT *
FROM {{ ref('int_offres_all_sources_source') }}
WHERE offer_id IS NOT NULL

{% endsnapshot %}
