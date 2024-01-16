with etp_sum as (
    select
        (
            select sum(emi_part_etp)
            from {{ source('fluxIAE', 'fluxIAE_EtatMensuelIndiv') }}
            where
                emi_sme_annee >= 2021
        ) as theirs,
        (
            select sum(emi_part_etp)
            from {{ ref("fluxIAE_EtatMensuelIndiv_v2") }}
        ) as ours
)

select *
from etp_sum
where round(theirs) != round(ours)
