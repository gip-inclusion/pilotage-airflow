with valid_answers as (

    select answer_id
    from {{ ref('esat__survey_answers_core') }}

),

source as (

    select
        source.answer_id,
        source.finess_num,
        source.software_financial_help
    from {{ ref('int_esat__surveys_esat_answers_deduplicated') }} as source
    inner join valid_answers
        on source.answer_id = valid_answers.answer_id
    where source.software_financial_help is not null

),

cleaned as (

    select
        answer_id,
        finess_num,

        trim(
            both '[]' from replace(software_financial_help, '''', '')
        ) as software_financial_help_cleaned
    from source
    where trim(software_financial_help) <> ''

),

exploded as (

    select
        answer_id,
        finess_num,
        trim(extracted_value) as software_financial_help_item
    from cleaned,
        unnest(
            string_to_array(software_financial_help_cleaned, ',')
        ) as extracted_value

)

select
    answer_id,
    finess_num,
    software_financial_help_item as software_financial_help
from exploded
where software_financial_help_item <> ''
