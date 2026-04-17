with source as (

    select
        finess_num,
        documents_falclist
    from {{ ref('int_esat__surveys_esat_answers_deduplicated') }}
    where
        documents_falclist is not null
        and trim(documents_falclist) <> ''

),

cleaned as (

    select
        finess_num,
        trim(
            both '[]' from replace(documents_falclist, '''', '')
        ) as documents_falclist_list
    from source

),

exploded as (

    select
        finess_num,
        trim(extracted_value) as document_falclist_item
    from cleaned,
        unnest(string_to_array(documents_falclist_list, ',')) as extracted_value

)

select
    finess_num,
    document_falclist_item
from exploded
where document_falclist_item <> ''
