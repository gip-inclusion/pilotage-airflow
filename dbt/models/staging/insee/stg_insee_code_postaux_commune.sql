with src as (
    select * from {{ref('seed_insee_code_postaux')}}
)

