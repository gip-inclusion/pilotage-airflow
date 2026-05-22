with valid_answers as (

    select answer_id
    from {{ ref('esat__survey_answers_core') }}

),

source as (

    select
        source.answer_id,
        source.finess_num,
        source.support_themes
    from {{ ref('int_esat__surveys_esat_answers_deduplicated') }} as source
    inner join valid_answers
        on source.answer_id = valid_answers.answer_id
    where
        source.support_themes is not null
        and trim(source.support_themes) <> ''

),

cleaned as (

    select
        answer_id,
        finess_num,
        trim(
            both '[]' from replace(support_themes, '''', '')
        ) as support_theme_list
    from source

),

exploded as (

    select
        answer_id,
        finess_num,
        trim(extracted_value) as support_theme
    from cleaned,
        unnest(string_to_array(support_theme_list, ',')) as extracted_value

)

select
    answer_id,
    finess_num,
    support_theme
from exploded
where support_theme <> ''
