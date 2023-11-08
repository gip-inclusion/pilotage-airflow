{% snapshot etp_conventionne_snapshot %}

    {{
        config(
          target_schema='public',
          strategy='check',
          unique_key='id_annexe_financiere',
          check_cols=['af_numero_avenant_modification','effectif_mensuel_conventionné', 'effectif_annuel_conventionné'],
          invalidate_hard_deletes=True,
        )
    }}

    select * from {{ ref('stg_etp_conventionnes') }}
    where annee_af = date_part('year', current_date)
{% endsnapshot %}
