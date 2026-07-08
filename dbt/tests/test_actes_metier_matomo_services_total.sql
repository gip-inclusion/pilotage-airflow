with final as (
    select
        mois,
        sum(nombre_actes) as nombre_actes
    from {{ ref('matomo__actes_metier') }}
    where type_acte = 'Recherche d’offre de service, hors emploi solidaire'
    group by mois
),

raw as (
    select
        mois,
        nb_visits as nombre_actes
    from {{ ref('stg_matomo__monthly_visits') }}
    where segment = 'pageUrl=@/search/services/results'
)

select
    coalesce(final.mois, raw.mois)  as mois,
    coalesce(final.nombre_actes, 0) as final_nombre_actes,
    coalesce(raw.nombre_actes, 0)   as raw_nombre_actes
from final
full outer join raw
    on final.mois = raw.mois
where coalesce(final.nombre_actes, 0) != coalesce(raw.nombre_actes, 0)
