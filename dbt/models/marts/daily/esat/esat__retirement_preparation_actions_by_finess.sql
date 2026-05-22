with valid_answers as (

    select answer_id
    from {{ ref('esat__survey_answers_core') }}

),

source as (

    select
        source.answer_id,
        source.finess_num,
        source.retirement_preparation_actions
    from {{ ref('int_esat__surveys_esat_answers_deduplicated') }} as source
    inner join valid_answers
        on source.answer_id = valid_answers.answer_id
    where
        source.retirement_preparation_actions is not null
        and trim(source.retirement_preparation_actions) <> ''

),

cleaned as (

    select
        answer_id,
        finess_num,
        trim(
            both '[]' from replace(retirement_preparation_actions, '''', '')
        ) as retirement_preparation_action_list
    from source

),

exploded as (

    select
        answer_id,
        finess_num,
        trim(extracted_value) as retirement_preparation_action
    from cleaned,
        unnest(
            string_to_array(retirement_preparation_action_list, ',')
        ) as extracted_value

)

select
    answer_id,
    finess_num,
    retirement_preparation_action
from exploded
where retirement_preparation_action <> ''
