with source as (

    select
        finess_num,
        retirement_preparation_actions
    from {{ ref('int_esat__surveys_esat_answers_deduplicated') }}
    where
        retirement_preparation_actions is not null
        and trim(retirement_preparation_actions) <> ''

),

cleaned as (

    select
        finess_num,
        trim(
            both '[]' from replace(retirement_preparation_actions, '''', '')
        ) as retirement_preparation_action_list
    from source

),

exploded as (

    select
        finess_num,
        trim(extracted_value) as retirement_preparation_action
    from cleaned,
        unnest(string_to_array(retirement_preparation_action_list, ',')) as extracted_value

)

select
    finess_num,
    retirement_preparation_action
from exploded
where retirement_preparation_action <> ''
