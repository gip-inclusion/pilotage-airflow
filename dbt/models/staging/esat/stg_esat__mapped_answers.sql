with mapped_answers as (

    select *
    from {{ ref('seed_esat__mapping_reponses_manuelles') }}

)

select
    establishment_finess_num::text as finess_num,
    answer_id
from mapped_answers
