select
    {{ pilo_star(source('emplois', 'pass_agréments'), relation_alias="pa") }},
    struct.bassin_d_emploi as bassin_emploi_structure,
    suspensions.motif_suspension,
    case
        when
            pa.date_fin >= current_date then 'pass valide'
        else 'pass non valide'
    end                    as validite_pass,
    case
        when
            suspensions."id_pass_agrément" is null then 'Non'
        else 'Oui'
    end                    as suspension_en_cours
from
    {{ source('emplois', 'pass_agréments') }} as pa
left join {{ ref('stg_structures') }} as struct
    on pa.id_structure = struct.id
left join {{ ref('eph_suspensions_pass_en_cours') }} as suspensions
    on pa.id = suspensions."id_pass_agrément"
where pa.type != 'Agrément PE'
