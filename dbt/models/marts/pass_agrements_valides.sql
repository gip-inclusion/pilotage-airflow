select
    {{ pilo_star(source('emplois', 'pass_agréments'), relation_alias="pa") }},
    case
        when
            pa.date_fin >= current_date then 'pass valide'
        else 'pass non valide'
    end as validite_pass
from
    {{ source('emplois', 'pass_agréments') }} as pa
