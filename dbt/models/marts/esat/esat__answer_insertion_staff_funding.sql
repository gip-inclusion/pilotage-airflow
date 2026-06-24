with source as (

    select
        answer_id,
        insertion_staff_funding
    from {{ ref('fct_esat__survey_answers') }}
    where
        insertion_staff_funding is not null
        and trim(insertion_staff_funding) <> ''

),

cleaned as (

    select
        answer_id,
        trim(
            both '[]' from replace(insertion_staff_funding, '''', '')
        ) as insertion_staff_funding_list
    from source

),

exploded as (

    select
        answer_id,
        trim(extracted_value) as insertion_staff_funding_item
    from cleaned,
        unnest(
            string_to_array(insertion_staff_funding_list, ',')
        ) as extracted_value

)

select
    answer_id,
    insertion_staff_funding_item
from exploded
where insertion_staff_funding_item <> ''
