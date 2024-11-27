with mp as (
    select
        (
            select sum(mpu_sct_mt_recet_nettoyage)
            from {{ source('fluxIAE', 'fluxIAE_MarchesPublics') }}
        ) as theirs,
        (
            select sum(mpu_sct_mt_recet_nettoyage)
            from {{ ref("fluxIAE_MarchesPublics_v2") }}
        ) as ours
)

select *
from mp
where round(theirs) != round(ours)
