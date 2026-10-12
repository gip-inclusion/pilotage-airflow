with row_counts as (

    select
        (select count(*) from {{ ref('dim_emplois__etablissements') }})  as establishments_count,
        (select count(*) from {{ ref('stg_emplois__structures') }})
        + (select count(*) from {{ ref('stg_emplois__organisations') }}) as structures_organisations_count

)

select *
from row_counts
where establishments_count != structures_organisations_count
