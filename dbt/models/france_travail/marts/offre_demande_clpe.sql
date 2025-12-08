select
    {{ pilo_star(ref('int_offre_demande_clpe')) }},
    part_de_besoins - part_de_services as tension,
    -- added to allow an easier filtering on metabase
    case
        when frein = 'total freins' then 'tension non pertinente'
        else 'tension pertinente'
    end                                as filtre_tension
from {{ ref('int_offre_demande_clpe') }}
