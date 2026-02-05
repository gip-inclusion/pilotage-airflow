with sum_freins as (
    select
        (
            select sum(nbr_freins_declares)
            from {{ ref('int_nombre_freins_agg') }}
        ) as freins_declares_agg,
        (
            select
                sum(
                    frein_numerique + frein_mobilite + frein_familial + frein_sante
                    + frein_savoir + frein_logement + frein_financier + frein_admin_jur
                )
            from {{ ref('stg_ft_data_diag') }}
        ) as freins_originaux
)

select *
from sum_freins
where freins_declares_agg != freins_originaux
