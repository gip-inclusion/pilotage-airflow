select
    candidats.id as emplois_candidat_id,
    ctr.contrat_id_structure,
    ctr.type_structure_emplois,
    ctr.contrat_id_ctr,
    ctr.contrat_parent_id,
    ctr.contrat_date_embauche,
    ctr.contrat_date_fin_contrat,
    ctr.contrat_date_sortie_definitive,
    case
        when ctr.num_reconduction = 0 then 'initial'
        when ctr.num_reconduction > 0 then 'renouvellement'
    end          as type_contrat
from {{ ref("stg_contrats") }} as ctr
inner join {{ ref("stg_uniques_salarie_id") }} as salarie
    on ctr.contrat_id_pph = salarie.salarie_id
inner join {{ source('emplois', 'candidats_v0') }} as candidats
    on salarie.hash_nir = candidats.hash_nir
