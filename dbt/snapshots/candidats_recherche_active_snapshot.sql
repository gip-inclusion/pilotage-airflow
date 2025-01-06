{% snapshot candidats_recherche_active_snapshot %}

    {{
        config(
          target_schema='public',
          strategy='check',
          unique_key='id',
          check_cols=['date_derniere_candidature', 'date_derniere_embauche','date_derniere_candidature_acceptee'],
          invalidate_hard_deletes=True,
        )
    }}

    select
        {{ pilo_star(ref('candidats_recherche_active'), except=['delai_derniere_candidature_interval_order']) }}
    from {{ ref('candidats_recherche_active') }}
{% endsnapshot %}
