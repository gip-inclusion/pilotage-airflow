with structures as (
    select * from {{ ref('int_di_structures_geo') }}
)

select * from structures
