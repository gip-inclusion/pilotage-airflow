with nb_structures as (
    select
        (
            select count(*)
            from {{ source('fluxIAE',"fluxIAE_Structure") }}
        ) as structure_all,
        (
            select count(distinct structure_id_siae)
            from {{ source('fluxIAE',"fluxIAE_Structure") }}
        ) as structure_distinct
)

select *
from nb_structures
where structure_all != structure_distinct
