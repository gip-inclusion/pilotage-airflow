select
    {{ pilo_star(ref('stg_candidatures'), relation_alias='candidatures') }},
    {{ pilo_star(ref('stg_reseaux'), except=["SIRET", "id_structure"], relation_alias='rsx') }},
    case
        when candidatures.injection_ai = 0 then 'Non'
        else 'Oui'
    end as reprise_de_stock_ai
from
    {{ ref('stg_candidatures') }} as candidatures
left join {{ ref('stg_reseaux') }} as rsx
    on candidatures.id_structure = rsx.id_structure
where candidatures.type_structure in ('AI', 'ACI', 'EITI', 'ETTI', 'EI')
