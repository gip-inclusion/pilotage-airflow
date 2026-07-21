with source as (

    select *
    from {{ ref('seed_esat__answer_field_exclusions') }}

)

select
    nullif(trim(answer_id::text), '')         as answer_id,
    lower(nullif(trim(field_name::text), '')) as field_name,
    nullif(trim(reason), '')                  as reason,
    nullif(trim(evidence), '')                as evidence
from source
