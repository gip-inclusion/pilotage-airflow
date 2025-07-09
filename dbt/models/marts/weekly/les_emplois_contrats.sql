select
    candidats.id             as candidat_id,
    salarie.hash_nir,
    ctr.contrat_id_structure as id_structure,
    structs.structure_denomination,
    structs.structure_siret_actualise,
    type_structure.type_structure_emplois,
    salarie.salarie_id,
    ctr.contrat_id_ctr,
    ctr.contrat_date_sortie_definitive,
    ctr.contrat_date_embauche,
    ctr.contrat_date_fin_contrat,
    case
        when ctr.contrat_type_contrat = 0 then 'contrat_initiale'
        when ctr.contrat_type_contrat > 0 then 'renouvellement'
    end                      as type_contrat
from {{ ref("fluxIAE_ContratMission_v2") }} as ctr
left join {{ ref('ref_mesure_dispositif_asp') }} as type_structure
    on ctr.contrat_mesure_disp_code = type_structure.af_mesure_dispositif_code
left join {{ ref("fluxIAE_Structure_v2") }} as structs
    on ctr.contrat_id_structure = structs.structure_id_siae
left join {{ ref("stg_uniques_salarie_id") }} as salarie
    on ctr.contrat_id_pph = salarie.salarie_id
left join {{ source('emplois', 'candidats_v0') }} as candidats
    on salarie.hash_nir = candidats.hash_nir
where candidats.id is not null
