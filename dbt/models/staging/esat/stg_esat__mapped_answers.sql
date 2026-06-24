with mapped_answers as (

    select *
    from {{ ref('seed_esat__mapping_reponses_manuelles') }}

)

select
    trim(establishment_finess_num::text) as finess_num,
    trim(answer_id::text)                as answer_id
from mapped_answers
