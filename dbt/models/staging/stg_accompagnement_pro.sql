select
    acc_id,
    acc_afi_id,
    annee_creation_accompagnement,
    type_acc_pro,
    sum(acc_det_nb_sal_int)                                               as acc_det_nb_sal_int,
    sum(acc_det_nb_sal_ext)                                               as acc_det_nb_sal_ext,
    sum(acc_det_nb_sal_int_ext)                                           as acc_det_nb_sal_int_ext,
    sum(acc_det_nb_sal_int + acc_det_nb_sal_ext + acc_det_nb_sal_int_ext) as acc_det_nb_sal_acc,
    case
        when (sum(acc_det_nb_sal_int + acc_det_nb_sal_ext + acc_det_nb_sal_int_ext)) = 0 then 0
        else cast(sum(acc_det_nb_sal_int) as float) / cast(sum(acc_det_nb_sal_int + acc_det_nb_sal_ext + acc_det_nb_sal_int_ext) as float)
    end                                                                   as ratio_int_vs_acc,
    case
        when (sum(acc_det_nb_sal_int + acc_det_nb_sal_ext + acc_det_nb_sal_int_ext)) = 0 then 0
        else cast(sum(acc_det_nb_sal_ext) as float) / cast(sum(acc_det_nb_sal_int + acc_det_nb_sal_ext + acc_det_nb_sal_int_ext) as float)
    end                                                                   as ratio_ext_vs_acc,
    case
        when (sum(acc_det_nb_sal_int + acc_det_nb_sal_ext + acc_det_nb_sal_int_ext)) = 0 then 0
        else cast(sum(acc_det_nb_sal_int_ext) as float) / cast(sum(acc_det_nb_sal_int + acc_det_nb_sal_ext + acc_det_nb_sal_int_ext) as float)
    end                                                                   as ratio_int_ext_vs_acc
from {{ ref("eph_accompagnement_pro") }}
group by acc_id, acc_afi_id, annee_creation_accompagnement, type_acc_pro
