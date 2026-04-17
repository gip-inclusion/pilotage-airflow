with source as (

    select
        finess_num,
        insertion_staff_funding
    from {{ ref('int_esat__surveys_esat_answers_deduplicated') }}
    where
        insertion_staff_funding is not null
        and trim(insertion_staff_funding) <> ''

),

cleaned as (

    select
        finess_num,
        trim(
            both '[]' from replace(insertion_staff_funding, '''', '')
        ) as insertion_staff_funding_list
    from source

),

exploded as (

    select
        finess_num,
        trim(extracted_value) as insertion_staff_funding_item
    from cleaned,
        unnest(string_to_array(insertion_staff_funding_list, ',')) as extracted_value

)

select
    finess_num,
    insertion_staff_funding_item
from exploded
where insertion_staff_funding_item <> ''
