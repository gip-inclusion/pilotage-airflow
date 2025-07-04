select
    structs.structure_id_siae,
    structs.structure_denomination,
    structs.structure_siret_actualise,
    type_structure.type_structure_emplois,
    salarie.salarie_id,
    ctr.contrat_id_ctr,
    ctr.contrat_date_sortie_definitive,
    ctr.contrat_date_embauche,
    ctr.contrat_date_fin_contrat,
    pass_ag."hash_numéro_pass_iae",
    pass_ag.id           as pass_iae_id,
    pass_ag."date_début" as pass_iae_date_debut,
    pass_ag.date_fin     as pass_iae_date_fin,
    case
        when ctr.contrat_type_contrat = 0 then 'contrat_initiale'
        when ctr.contrat_type_contrat > 0 then 'renouvellement'
    end                  as type_contrat
from {{ ref("fluxIAE_ContratMission_v2") }} as ctr
left join {{ ref('ref_mesure_dispositif_asp') }} as type_structure
    on ctr.contrat_mesure_disp_code = type_structure.af_mesure_dispositif_code
left join {{ ref("fluxIAE_Structure_v2") }} as structs
    on ctr.contrat_id_structure = structs.structure_id_siae
left join {{ source('fluxIAE',"fluxIAE_Salarie") }} as salarie
    on ctr.contrat_id_pph = salarie.salarie_id
left join {{ source('emplois', 'pass_agréments') }} as pass_ag
    on salarie."hash_numéro_pass_iae" = pass_ag."hash_numéro_pass_iae"
where
    pass_ag.type != 'Agrément PE'
    and pass_ag."date_début" <= ctr.contrat_date_embauche
    and pass_ag.date_fin >= to_date(ctr.contrat_date_sortie_definitive, 'DD/MM/YYYY')
