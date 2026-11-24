with couverture as (
    select
        count(*)                             as nombre_lignes,
        count(distinct date_jour)            as nombre_jours,
        min(date_jour)                       as premiere_date,
        max(date_jour)                       as derniere_date,
        count(*) filter (where date_id = -1) as nombre_inconnues
    from {{ ref('dim_date') }}
),

anomalies as (
    select 'couverture' as anomalie
    from couverture
    where
        nombre_lignes != 20455
        or nombre_jours != 20454
        or premiere_date is distinct from date '1990-01-01'
        or derniere_date is distinct from date '2045-12-31'
        or nombre_inconnues != 1

    union all

    select 'attributs : ' || dimension.date_id as anomalie
    from {{ ref('dim_date') }} as dimension
    where
        (
            date_id = -1
            and (
                est_date_inconnue is distinct from true
                or libelle_date is distinct from 'Date inconnue'
                or jsonb_strip_nulls(
                    to_jsonb(dimension) - array['date_id', 'libelle_date', 'est_date_inconnue']
                ) != '{}'::jsonb
            )
        )
        or (
            date_id != -1
            and (
                est_date_inconnue is distinct from false
                or date_id is distinct from to_char(date_jour, 'YYYYMMDD')::integer
                or exists (
                    select 1
                    from jsonb_each(to_jsonb(dimension)) as attribut
                    where attribut.value = 'null'::jsonb
                )
            )
        )
)

select *
from anomalies
