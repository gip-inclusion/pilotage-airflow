with multiple_contrat_parent_id as (
    select
        contrat_parent_id,
        count(*) as count_occurences
    from {{ ref('contrats_parent') }}
    group by contrat_parent_id
    having count(*) > 1
)

select *
from multiple_contrat_parent_id
where count_occurences > 10
