with valid_answers as (

    select answer_id
    from {{ ref('esat__survey_answers_core') }}

),

source as (

    select
        source.answer_id,
        source.finess_num,
        source.skills_validation_type
    from {{ ref('int_esat__surveys_esat_answers_deduplicated') }} as source
    inner join valid_answers
        on source.answer_id = valid_answers.answer_id
    where
        source.skills_validation_type is not null
        and trim(source.skills_validation_type) <> ''

),

cleaned as (

    select
        answer_id,
        finess_num,
        trim(
            both '[]' from replace(skills_validation_type, '''', '')
        ) as skills_validation_type_list
    from source

),

exploded as (

    select
        answer_id,
        finess_num,
        trim(extracted_value) as skills_validation_type
    from cleaned,
        unnest(string_to_array(skills_validation_type_list, ',')) as extracted_value

)

select
    answer_id,
    finess_num,
    skills_validation_type
from exploded
where skills_validation_type <> ''
