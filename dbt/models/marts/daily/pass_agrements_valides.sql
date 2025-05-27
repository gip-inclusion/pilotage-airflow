select
    {{ pilo_star(source('emplois', 'pass_agréments'), relation_alias="pa") }},
    struct.bassin_d_emploi as bassin_emploi_structure,
    case
        when
            pa.date_fin >= current_date then 'pass valide'
        else 'pass non valide'
    end                    as validite_pass,
    suspensions.suspension_en_cours,
    suspensions.motif_suspension
from
    {{ source('emplois', 'pass_agréments') }} as pa
left join {{ ref('stg_structures') }} as struct
    on struct.id = pa.id_structure
left join {{ ref('suspensions_pass') }} as suspensions
    on suspensions."id_pass_agrément" = pa.id
