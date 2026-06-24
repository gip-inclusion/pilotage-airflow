with source as (

    select
        answer_id,
        retirement_preparation_actions
    from {{ ref('fct_esat__survey_answers') }}
    where
        retirement_preparation_actions is not null
        and trim(retirement_preparation_actions) <> ''

),

cleaned as (

    select
        answer_id,
        trim(
            both '[]' from replace(retirement_preparation_actions, '''', '')
        ) as retirement_preparation_action_list
    from source

),

exploded as (

    select
        answer_id,
        trim(extracted_value) as retirement_preparation_action
    from cleaned,
        unnest(
            string_to_array(retirement_preparation_action_list, ',')
        ) as extracted_value

)

select
    answer_id,
    retirement_preparation_action
from exploded
where retirement_preparation_action <> ''
