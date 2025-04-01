-- en date 20/03 28 fois on arrive pas à joindre l'annexe financiere et par consequence la structure
--
select
    {{ pilo_star(ref('stg_accompagnement_pro'), relation_alias="t_acc_pro" ) }},
    acc_pro_libelle.libelle,
    {{ pilo_star(ref('stg_info_per_annexe_financiere')) }},
    t_struct.structure_id_siae                      as structure_id,
    t_struct.structure_denomination,
    t_struct.structure_denomination_unique,
    t_struct.nom_departement_structure,
    t_struct.code_dept_structure                    as numero_departement_structure,
    t_struct.nom_region_structure,
    extract(year from t_afi.af_date_debut_effet_v2) as annee_af
from {{ ref('stg_accompagnement_pro') }} as t_acc_pro
left join {{ ref('stg_info_per_annexe_financiere') }} as t_afi on t_acc_pro.acc_afi_id = t_afi.af_id_annexe_financiere
left join {{ ref('fluxIAE_Structure_v2') }} as t_struct on t_afi.af_id_structure = t_struct.structure_id_siae
left join {{ ref('acc_pro_libelle') }} on t_acc_pro.type_acc_pro = acc_pro_libelle.type_acc_pro
