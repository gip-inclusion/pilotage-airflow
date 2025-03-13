select
    acc_id,
    acc_afi_id,
    annee_creation_accompagnement,
    type_frein,
    sum(acc_dif_nb_sal_conc)                                              as acc_dif_nb_sal_conc,
    sum(acc_dif_nb_sal_int)                                               as acc_dif_nb_sal_int,
    sum(acc_dif_nb_sal_ext)                                               as acc_dif_nb_sal_ext,
    sum(acc_dif_nb_sal_int_ext)                                           as acc_dif_nb_sal_int_ext,
    sum(acc_dif_nb_sal_int + acc_dif_nb_sal_ext + acc_dif_nb_sal_int_ext) as acc_dif_nb_sal_acc
from {{ ref("eph_accompagnement_freins") }}
group by acc_id, acc_afi_id, annee_creation_accompagnement, type_frein
