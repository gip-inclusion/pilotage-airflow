select
    form.formation_id_ctr,
    form.formation_id_for,
    form.formation_date_debut,
    form.formation_date_fin,
    form.formation_duree_jours,
    form.formation_duree_heures,
    ctr.type_structure_emplois,
    ctr.nom_departement_structure,
    ctr.structure_siret_actualise,
    ctr.nom_region_structure,
    ctr.code_dept_structure,
    ctr.nombre_heures_travail_non_zero,
    ctr.structure_denomination_unique,
    ref_obj.rof_libelle                                as formation_objectif_libelle,
    ref_type.rtf_libelle                               as formation_type_libelle,
    extract(year from form.formation_date_debut::date) as formation_annee_debut
from {{ source('fluxIAE','fluxIAE_Formations') }} as form
left join {{ ref('stg_contrats') }} as ctr
    on form.formation_id_ctr = ctr.contrat_id_ctr
left join {{ source('fluxIAE','fluxIAE_RefObjectifFormation') }} as ref_obj
    on ctr.contrat_mesure_disp_id = ref_obj.rme_id and form.formation_objectif_for_code = ref_obj.rof_code_formation
left join {{ source('fluxIAE','fluxIAE_RefTypeFormation') }} as ref_type
    on form.formation_type_for_code = ref_type.rtf_code_formation and ctr.contrat_mesure_disp_id = ref_type.rme_id
