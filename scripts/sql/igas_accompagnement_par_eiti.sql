select
    structure.structure_id_siae id_struct,
    date_part('year', af.af_date_debut_effet_v2) as annee_af,
    sum(acc_dif_nb_sal_conc_illetrisme) nb_salaries_concernes_illetrisme,
    sum(acc_dif_nb_sal_int_illetrisme) nb_salaries_accompagnes_illetrisme_int,
    sum(acc_dif_nb_sal_ext_illetrisme) nb_salaries_accompagnes_illetrisme_ext,
    sum(acc_dif_nb_sal_conc_sante) nb_salaries_concernes_sante,
    sum(acc_dif_nb_sal_int_sante) nb_salaries_accompagnes_sante_int,
    sum(acc_dif_nb_sal_ext_sante) nb_salaries_accompagnes_sante_ext,
    sum(acc_dif_nb_sal_conc_heberg) nb_salaries_concernes_hebergement,
    sum(acc_dif_nb_sal_int_heberg) nb_salaries_accompagnes_hebergement_int,
    sum(acc_dif_nb_sal_ext_heberg) nb_salaries_accompagnes_hebergement_ext,
    sum(acc_dif_nb_sal_conc_demarch) nb_salaries_concernes_demarches_admin,
    sum(acc_dif_nb_sal_int_demarch) nb_salaries_accompagnes_demarches_admin_int,
    sum(acc_dif_nb_sal_ext_demarch) nb_salaries_accompagnes_demarches_admin_ext,
    sum(acc_dif_nb_sal_conc_mobilite) nb_salaries_concernes_mobilite,
    sum(acc_dif_nb_sal_int_mobilite) nb_salaries_accompagnes_mobilite_int,
    sum(acc_dif_nb_sal_ext_mobilite) nb_salaries_accompagnes_mobilite_ext,
    sum(acc_dif_nb_sal_conc_surendet) nb_salaries_concernes_surendetement,
    sum(acc_dif_nb_sal_int_surendet) nb_salaries_accompagnes_surendetement_int,
    sum(acc_dif_nb_sal_ext_surendet) nb_salaries_accompagnes_surendetement_ext,
    sum(acc_dif_nb_sal_ext_justice) nb_salaries_concernes_justice_ext,
    sum(acc_dif_nb_sal_int_justice) nb_salaries_concernes_justice_int,
    sum(acc_dif_nb_sal_conc_manque_dispo) nb_salaries_concernes_manque_dispo,
    sum(acc_dif_nb_sal_int_manque_dispo) nb_salaries_accompagnes_manque_dispo_int,
    sum(acc_dif_nb_sal_ext_manque_dispo) nb_salaries_accompagnes_manque_dispo_ext
from
    "FluxIAE_accompagnement" acc
    left join "fluxIAE_AnnexeFinanciere_v2" as af on af.af_id_annexe_financiere = acc.acc_afi_id
    left join "fluxIAE_Structure_v2" as structure on af.af_id_structure = structure.structure_id_siae
where
    af.type_siae = 'EITI'
group by
    structure.structure_id_siae,
    annee_af;