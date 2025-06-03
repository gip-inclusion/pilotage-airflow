select
    ctr.contrat_id_pph,
    ctr.contrat_id_ctr,
    ctr.contrat_id_structure,
    type_structure.type_structure_emplois,
    ctr.contrat_mesure_disp_code,
    ctr.contrat_mesure_disp_id,
    ctr.contrat_duree_contrat,
    ctr.contrat_date_embauche,
    ctr.contrat_date_fin_contrat,
    ctr.contrat_type_contrat                                  as num_reconduction,
    rfc.rfc_lib_forme_contrat                                 as type_contrat,
    motif_sortie.rms_libelle                                  as motif_sortie,
    categorie_sortie.rcs_libelle                              as categorie_sortie,
    emi.emi_nb_heures_travail                                 as contrat_nb_heures,
    structs.nom_region_structure,
    structs.nom_departement_structure,
    structs.code_dept_structure,
    structs.structure_denomination_unique,
    structs.structure_siret_actualise,
    to_date(ctr.contrat_date_sortie_definitive, 'DD/MM/YYYY') as contrat_date_sortie_definitive,
    first_value(ctr.contrat_id_ctr) over (
        partition by ctr.contrat_id_pph, groupe_contrat, contrat_mesure_disp_code
        order by ctr.contrat_date_embauche
    )                                                         as contrat_parent_id,
    case
        when
            emi.emi_nb_heures_travail > 0
            then 'oui'
        else 'non'
    end                                                       as nombre_heures_travail_non_zero
from {{ ref('fluxIAE_ContratMission_v2') }} as ctr
left join {{ ref('eph_heures_travail_contrat') }} as emi
    on ctr.contrat_id_ctr = emi.emi_ctr_id
left join {{ source("fluxIAE", "fluxIAE_RefFormeContrat") }} as rfc
    on ctr.contrat_format_contrat_code = rfc.rfc_id
left join {{ ref('fluxIAE_RefMotifSort_v2') }} as motif_sortie
    on emi.emi_motif_sortie_id = motif_sortie.rms_id
left join {{ ref("fluxIAE_RefCategorieSort_v2") }} as categorie_sortie
    on motif_sortie.rcs_id = categorie_sortie.rcs_id
left join {{ ref('ref_mesure_dispositif_asp') }} as type_structure
    on ctr.contrat_mesure_disp_code = type_structure.af_mesure_dispositif_code
left join {{ ref('fluxIAE_Structure_v2') }} as structs
    on ctr.contrat_id_structure = structs.structure_id_siae
left join {{ ref('stg_group_contrats') }} as groupe
    on ctr.contrat_id_ctr = groupe.contrat_id_ctr
