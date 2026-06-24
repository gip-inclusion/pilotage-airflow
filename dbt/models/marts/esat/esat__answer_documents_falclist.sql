with source as (

    select
        answer_id,
        documents_falclist
    from {{ ref('fct_esat__survey_answers') }}
    where
        documents_falclist is not null
        and trim(documents_falclist) <> ''

),

cleaned as (

    select
        answer_id,
        trim(
            both '[]' from replace(documents_falclist, '''', '')
        ) as documents_falclist_list
    from source

),

exploded as (

    select
        answer_id,
        trim(extracted_value) as document_falclist_item
    from cleaned,
        unnest(
            string_to_array(documents_falclist_list, ',')
        ) as extracted_value

)

select
    answer_id,
    document_falclist_item
from exploded
where document_falclist_item <> ''
