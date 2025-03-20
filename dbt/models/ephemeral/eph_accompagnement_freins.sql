select
    t_acc.acc_id,
    t_acc.acc_afi_id,
    t_acc.annee_creation_accompagnement,
    case when key like 'acc_dif_nb_sal_conc%' then coalesce(value::integer, 0) else 0 end                                           as acc_nb_sal_conc,
    case when key like 'acc_dif_nb_sal_int%' and key not like 'acc_dif_nb_sal_int_ext%' then coalesce(value::integer, 0) else 0 end as acc_nb_sal_int,
    case when key like 'acc_dif_nb_sal_ext%' then coalesce(value::integer, 0) else 0 end                                            as acc_nb_sal_ext,
    case when key like 'acc_dif_nb_sal_int_ext%' then coalesce(value::integer, 0) else 0 end                                        as acc_nb_sal_int_ext,
    trim(substring(key from '^(?:acc_dif_nb_sal_conc|acc_dif_nb_sal_int|acc_dif_nb_sal_int_ext|acc_dif_nb_sal_ext)_(.*)$'))         as type_frein
from {{ ref("stg_fluxIAE_Accompagnement") }} as t_acc,
    lateral jsonb_each_text(
        to_jsonb(t_acc)
        - 'acc_id'
        - 'acc_afi_id'
        - 'acc_date_creation'
        - 'annee_creation_accompagnement'
    )
where key like 'acc_dif_nb_sal%'
