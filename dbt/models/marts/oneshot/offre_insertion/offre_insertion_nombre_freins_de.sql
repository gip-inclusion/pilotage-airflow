select
    {{ dbt_utils.star(ref('fct_nombre_freins_agg'), relation_alias = 'oi') }},
    clpe.departement_id,
    clpe.nom_departement,
    clpe.nom_region,
    case
        when clpe.nom_departement is not null then concat(clpe.departement_id, '-', clpe.nom_departement)
    end as departement
from {{ ref("fct_nombre_freins_agg") }} as oi
left join {{ ref('dim_clpe_ft') }} as clpe
    on oi.territoire_id = clpe.territoire_id
