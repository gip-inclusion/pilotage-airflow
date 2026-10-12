with row_counts as (

    select
        (select count(uid_structure) from {{ ref('dim_emplois__etablissements') }})    as structures_count,
        (select count(*) from {{ ref('stg_emplois__structures') }})                    as stg_structures_count,
        (select count(uid_organisation) from {{ ref('dim_emplois__etablissements') }}) as organisations_count,
        (select count(*) from {{ ref('stg_emplois__organisations') }})                 as stg_organisations_count

)

select *
from row_counts
where
    structures_count != stg_structures_count
    or organisations_count != stg_organisations_count
