{% snapshot offre_insertion_couverture_snapshot %}

    {{
        config(
          target_schema='public',
          strategy='check',
          unique_key=['id_clpe', 'type_frein'],
          check_cols=['date_extraction'],
          invalidate_hard_deletes=True,
        )
    }}

    select * from {{ ref('offre_insertion_couverture') }}
{% endsnapshot %}
