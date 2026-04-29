with services as (
    select * from {{ ref('int_di_services_geo') }}
)

select * from services
