{% snapshot offre_insertion_nombre_freins_de_snapshot %}

    {{
        config(
          target_schema='public',
          strategy='check',
          unique_key=['territoire_id', 'type_frein'],
          check_cols=['date_extraction'],
          invalidate_hard_deletes=True,
        )
    }}

    select * from {{ ref('offre_insertion_nombre_freins_de') }}
{% endsnapshot %}
