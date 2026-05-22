with valid_answers as (

    select answer_id
    from {{ ref('esat__survey_answers_core') }}

),

source as (

    select
        source.answer_id,
        source.finess_num,
        source.insertion_staff_funding
    from {{ ref('int_esat__surveys_esat_answers_deduplicated') }} as source
    inner join valid_answers
        on source.answer_id = valid_answers.answer_id
    where
        source.insertion_staff_funding is not null
        and trim(source.insertion_staff_funding) <> ''

),

cleaned as (

    select
        answer_id,
        finess_num,
        trim(
            both '[]' from replace(insertion_staff_funding, '''', '')
        ) as insertion_staff_funding_list
    from source

),

exploded as (

    select
        answer_id,
        finess_num,
        trim(extracted_value) as insertion_staff_funding_item
    from cleaned,
        unnest(
            string_to_array(insertion_staff_funding_list, ',')
        ) as extracted_value

)

select
    answer_id,
    finess_num,
    insertion_staff_funding_item
from exploded
where insertion_staff_funding_item <> ''
