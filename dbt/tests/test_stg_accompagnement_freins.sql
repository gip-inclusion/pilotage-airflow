select *
from {{ ref('stg_accompagnement_freins') }}
where acc_dif_nb_sal_acc > acc_diff_nb_sal_conc
