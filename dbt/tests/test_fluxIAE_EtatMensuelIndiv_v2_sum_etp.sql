with etp_sum as (
    select
        (
            select sum(emi_part_etp)
            from {{ source('fluxIAE', 'fluxIAE_EtatMensuelIndiv') }}
            where
                emi_sme_annee in
                (
                    date_part('year', current_date),
                    date_part('year', current_date) - 1,
                    date_part('year', current_date) - 2
                )
        ) as theirs,
        (
            select sum(emi_part_etp)
            from {{ ref("fluxIAE_EtatMensuelIndiv_v2") }}
        ) as ours
)

select *
from etp_sum
where round(theirs) != round(ours)
