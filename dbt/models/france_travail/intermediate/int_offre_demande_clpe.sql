with offre_demande as (
    select
    {{ pilo_star(ref('int_freins_iop_clpe'), relation_alias='iop') }},
    {{ pilo_star(ref('int_thematiques_clpe'), except=["territoire_id","territoire_libelle","serives"], relation_alias='di') }}
    from {{ ref('int_freins_iop_clpe') }} as iop
    left join {{ ref('int_thematiques_clpe') }} as di
        on iop.territoire_id = di.territoire_id and iop.frein = di.services
    where iop.territoire_id is not null
)

select
    territoire_id,
    territoire_libelle,
    departement_id,
    frein,
    nbr_demandeurs_emplois,
    nombre_de_freins,
    nombre_de_services,
    -- here we're going to take the total freins to be able to compute the ratios
    case
        when max(case when frein = 'total freins' then nombre_de_freins end) over (partition by territoire_libelle) > 0
            then round((nombre_de_freins / max(case when frein = 'total freins' then nbr_demandeurs_emplois end) over (partition by territoire_libelle)), 3)
        else 0
    end as part_de_besoins,
    case
        when max(case when frein = 'total freins' then nombre_de_services end) over (partition by territoire_libelle) > 0
            then round((nombre_de_services::numeric / max(case when frein = 'total freins' then nombre_de_services end) over (partition by territoire_libelle)), 3)
        else 0
    end as part_de_services
from offre_demande
order by territoire_libelle, frein
