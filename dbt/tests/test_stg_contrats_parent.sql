with multiple_contrat_parent_id as (
    select
        contrat_parent_id,
        count(*) as count
    from {{ ref('stg_contrats_parent') }}
    having count(*) > 1
)

select count(*)
from multiple_contrat_parent_id
where count(*) > 10
