select
    ctr.contrat_id_structure,
    ctr.contrat_id_pph,
    salarie.hash_nir,
    count(distinct ctr.type_contrat)                                    as types_contrats_distincts,
    array_agg(ctr.type_contrat order by ctr.contrat_date_embauche)      as types_contrats,
    (array_agg(ctr.type_contrat order by ctr.contrat_date_embauche))[1] as type_contrat_embauche,
    max(structs.code_dept_structure)                                    as departement_structure,
    max(structs.nom_departement_structure)                              as nom_departement_structure,
    max(structs.nom_region_structure)                                   as nom_region_structure,
    max(motif_sortie)                                                   as motif_sortie,
    case
        when max(motif_sortie) is null then 'Non'
        else 'Oui'
    end                                                                 as salarie_sorti,
    array_agg(ctr.contrat_id_ctr order by ctr.contrat_date_embauche)    as id_contrats,
    max(contrat_mesure_disp_code)                                       as type_aci,
    min(ctr.contrat_date_embauche)                                      as date_embauche_premier_contrat,
    max(ctr.contrat_date_fin_contrat)                                   as date_fin_dernier_contrat,
    max(ctr.contrat_date_sortie_definitive)                             as date_sortie_definitive_dernier_contrat,
    case
        when max(ctr.contrat_date_sortie_definitive) is not null then {{ duration_in_days('min(ctr.contrat_date_embauche)', 'max(ctr.contrat_date_sortie_definitive)') }}
        else {{ duration_in_days('min(ctr.contrat_date_embauche)', 'max(ctr.contrat_date_fin_contrat)') }}
    end                                                                 as duree_contrat_jours,
    case
        when max(ctr.contrat_date_sortie_definitive) is not null then {{ duration_in_months('min(ctr.contrat_date_embauche)', 'max(ctr.contrat_date_sortie_definitive)') }}
        else {{ duration_in_months('min(ctr.contrat_date_embauche)', 'max(ctr.contrat_date_fin_contrat)') }}
    end                                                                 as duree_contrat_mois,
    sum(ctr.contrat_duree_contrat)                                      as duree_contrat_asp_mois,
    max(struct_emplois.structure_convergence)                           as structure_convergence
from {{ ref('stg_contrats') }} as ctr
left join {{ ref("fluxIAE_Structure_v2") }} as structs
    on ctr.contrat_id_structure = structs.structure_id_siae
left join {{ ref('stg_structures') }} as struct_emplois
    on structs.structure_siret_actualise = cast(struct_emplois.siret as bigint)
left join {{ ref("fluxIAE_Salarie_v2") }} as salarie
    on ctr.contrat_id_pph = salarie.salarie_id
where (contrat_mesure_disp_code = 'ACI_DC' or contrat_mesure_disp_code = 'ACI_MP') and contrat_nb_heures > 0
group by
    ctr.groupe_contrat,
    salarie.hash_nir,
    ctr.contrat_id_structure,
    ctr.contrat_id_pph
