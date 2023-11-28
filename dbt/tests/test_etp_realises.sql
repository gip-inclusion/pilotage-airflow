with etp_realises as (
    select
        (
            select count(distinct emi_dsm_id)
            from {{ ref("suivi_etp_realises_v2") }}
        ) as count_distinct,
        (
            select count(*)
            from {{ ref("suivi_etp_realises_v2") }}
        ) as count_all
)

select *
from etp_realises
where count_all != count_distinct
