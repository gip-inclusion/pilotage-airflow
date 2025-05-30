select
    acc_id,
    acc_afi_id,
    annee_creation_accompagnement,
    type_frein,
    case
        when sum(acc_nb_sal_int + acc_nb_sal_ext + acc_nb_sal_int_ext) > sum(acc_nb_sal_conc) then sum(acc_nb_sal_int + acc_nb_sal_ext + acc_nb_sal_int_ext)
        else sum(acc_nb_sal_conc)
    end                                                       as acc_nb_sal_conc,
    sum(acc_nb_sal_int)                                       as acc_nb_sal_int,
    sum(acc_nb_sal_ext)                                       as acc_nb_sal_ext,
    sum(acc_nb_sal_int_ext)                                   as acc_nb_sal_int_ext,
    sum(acc_nb_sal_int + acc_nb_sal_ext + acc_nb_sal_int_ext) as acc_nb_sal_acc,
    case
        when (sum(acc_nb_sal_conc)) = 0 then 0
        else cast(sum(acc_nb_sal_int + acc_nb_sal_ext + acc_nb_sal_int_ext) as float) / cast(sum(acc_nb_sal_conc) as float)
    end                                                       as ratio_salaries_accompagnes,
    case
        when (sum(acc_nb_sal_int + acc_nb_sal_ext + acc_nb_sal_int_ext)) = 0 then 0
        else cast(sum(acc_nb_sal_int) as float) / cast(sum(acc_nb_sal_int + acc_nb_sal_ext + acc_nb_sal_int_ext) as float)
    end                                                       as ratio_int_vs_acc,
    case
        when (sum(acc_nb_sal_int + acc_nb_sal_ext + acc_nb_sal_int_ext)) = 0 then 0
        else cast(sum(acc_nb_sal_ext) as float) / cast(sum(acc_nb_sal_int + acc_nb_sal_ext + acc_nb_sal_int_ext) as float)
    end                                                       as ratio_ext_vs_acc,
    case
        when (sum(acc_nb_sal_int + acc_nb_sal_ext + acc_nb_sal_int_ext)) = 0 then 0
        else cast(sum(acc_nb_sal_int_ext) as float) / cast(sum(acc_nb_sal_int + acc_nb_sal_ext + acc_nb_sal_int_ext) as float)
    end                                                       as ratio_int_ext_vs_acc
from {{ ref("eph_accompagnement_freins") }}
group by acc_id, acc_afi_id, annee_creation_accompagnement, type_frein
