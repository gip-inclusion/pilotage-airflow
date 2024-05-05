select
    cgv_structs.siret,
    structs.structure_denomination,
    structs.nom_epci_structure,
    structs.nom_region_structure,
    structs.code_dept_structure,
    structs.nom_departement_structure,
    ctr.contrat_id_pph,
    ctr.contrat_id_ctr,
    ctr.contrat_duree_contrat,
    ctr.contrat_date_sortie_definitive as date_sortie,
    ctr.contrat_nb_heures              as nb_heures,
    case
        when cgv_structs.siret is not null and (ctr.contrat_mesure_disp_code = 'ACI_DC' or ctr.contrat_mesure_disp_code = 'ACI_MP') then 'Oui'
        else 'Non'
    end                                as structure_convergence
from {{ ref('stg_contrats') }} as ctr
left join {{ ref("fluxIAE_Structure_v2") }} as structs
    on ctr.contrat_id_structure = structs.structure_id_siae
left join {{ ref("sirets_structures_convergence") }} as cgv_structs
    on structs.structure_siret_signature = cast(cgv_structs.siret as bigint)
-- on ne garde que les contrats terminés en 2023
where ctr.contrat_duree_contrat is not null and extract(year from ctr.contrat_date_sortie_definitive) = 2023
