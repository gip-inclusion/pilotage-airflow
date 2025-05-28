select
    suspension."id_pass_agrément",
    suspension."date_début",
    suspension.date_fin,
    rms.label as motif_suspension,
    case
        when suspension.en_cours = 1 then 'Oui' else 'Non'
    end       as suspension_en_cours
from {{ source("emplois", "suspensions_v0") }} as suspension
left join {{ source("emplois", "c1_ref_motif_suspension") }} as rms
    on suspension.motif = rms.code
