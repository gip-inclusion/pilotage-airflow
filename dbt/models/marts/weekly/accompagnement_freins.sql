select
    {{ pilo_star(ref('stg_accompagnement_freins')) }},
    {{ pilo_star(ref('stg_info_per_annexe_financiere')) }},
    t_struct.structure_id_siae                      as structure_id,
    t_struct.structure_denomination,
    t_struct.nom_departement_structure,
    t_struct.code_dept_structure             as numero_departement_structure,
    t_struct.nom_region_structure,
    t_acc.acc_dif_nb_sal_acc/t_acc.acc_dif_nb_sal_conc as ratio_salaries_accompagnes,
    t_acc.acc_dif_nb_sal_int/t_acc.acc_dif_nb_sal_acc as ratio_int_vs_acc,
    t_acc.acc_dif_nb_sal_ext/t_acc.acc_dif_nb_sal_acc as ratio_ext_vs_acc,
    t_acc.acc_dif_nb_sal_int_ext/t_acc.acc_dif_nb_sal_acc as ratio_int_ext_vs_acc,
    extract(year from t_afi.af_date_debut_effet_v2) as annee_af
from {{ ref('stg_accompagnement_freins') }} as t_acc
left join {{ ref('stg_info_per_annexe_financiere') }} as t_afi on t_acc.acc_afi_id = t_afi.af_id_annexe_financiere
left join {{ ref('fluxIAE_Structure_v2') }} as t_struct on t_afi.af_id_structure = t_struct.structure_id_siae
