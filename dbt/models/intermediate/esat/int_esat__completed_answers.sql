with source as (

    select *
    from {{ ref('int_esat__surveys_esat_answers_deduplicated') }}

),

final as (

    select
        answer_id,
        completeness_score
    from source
    where completeness_score > 10

)

select *
from final
