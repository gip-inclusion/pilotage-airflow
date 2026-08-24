with mapped_answers as (

    select *
    from {{ ref('seed_esat__mapping_reponses_manuelles') }}

)

select
    lpad(nullif(trim(establishment_finess_num::text), ''), 9, '0') as finess_num,
    nullif(trim(answer_id::text), '')                              as answer_id
from mapped_answers
