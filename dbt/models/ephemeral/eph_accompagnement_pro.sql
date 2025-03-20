select
    t_acc.acc_id,
    t_acc.acc_afi_id,
    t_acc.annee_creation_accompagnement,
    case when key like '%_int' then coalesce(value::integer, 0) else 0 end                             as acc_nb_sal_int,
    case when key like '%_ext' and key not like '%int_ext' then coalesce(value::integer, 0) else 0 end as acc_nb_sal_ext,
    case when key like '%_int_ext' then coalesce(value::integer, 0) else 0 end                         as acc_nb_sal_int_ext,
    trim(regexp_replace(regexp_replace(key, '^acc_det_nb_', ''), '(_int|_ext|_int_ext)', ''))          as type_acc_pro
from {{ ref("stg_fluxIAE_Accompagnement") }} as t_acc,
    lateral jsonb_each_text(
        to_jsonb(t_acc)
        - 'acc_id'
        - 'acc_afi_id'
        - 'acc_date_creation'
        - 'annee_creation_accompagnement'
    )
where key like 'acc_det_nb_%'
