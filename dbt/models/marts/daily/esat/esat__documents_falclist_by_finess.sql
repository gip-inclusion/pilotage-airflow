with valid_answers as (

    select answer_id
    from {{ ref('esat__survey_answers_core') }}

),

source as (

    select
        source.answer_id,
        source.finess_num,
        source.documents_falclist
    from {{ ref('int_esat__surveys_esat_answers_deduplicated') }} as source
    inner join valid_answers
        on source.answer_id = valid_answers.answer_id
    where
        source.documents_falclist is not null
        and trim(source.documents_falclist) <> ''

),

cleaned as (

    select
        answer_id,
        finess_num,
        trim(
            both '[]' from replace(documents_falclist, '''', '')
        ) as documents_falclist_list
    from source

),

exploded as (

    select
        answer_id,
        finess_num,
        trim(extracted_value) as document_falclist_item
    from cleaned,
        unnest(
            string_to_array(documents_falclist_list, ',')
        ) as extracted_value

)

select
    answer_id,
    finess_num,
    document_falclist_item
from exploded
where document_falclist_item <> ''
