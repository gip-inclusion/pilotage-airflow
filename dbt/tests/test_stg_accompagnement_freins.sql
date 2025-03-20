select
    acc_id,
    type_frein,
    sum(acc_dif_nb_sal_acc)  as total_salaries,
    sum(acc_dif_nb_sal_conc) as total_salaries_conc,
    count(*)                 as nb
from {{ ref("stg_accompagnement_freins") }}
group by acc_id, type_frein
having count(*) > 1 or (sum(acc_dif_nb_sal_acc) > sum(acc_dif_nb_sal_conc))
