with forced_no_answer as (

    select *
    from {{ ref('seed_esat__forced_no_answer') }}

)

select
    lpad(nullif(trim(establishment_finess_num::text), ''), 9, '0') as finess_num,
    nullif(trim(reason::text), '')                                 as reason
from forced_no_answer
