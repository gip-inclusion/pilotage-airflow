with source as (

    select
        answer_id,
        software_financial_help
    from {{ ref('fct_esat__survey_answers') }}
    where
        software_financial_help is not null
        and trim(software_financial_help) <> ''

),

cleaned as (

    select
        answer_id,
        trim(
            both '[]' from replace(software_financial_help, '''', '')
        ) as software_financial_help_cleaned
    from source

),

exploded as (

    select
        answer_id,
        trim(extracted_value) as software_financial_help_item
    from cleaned,
        unnest(
            string_to_array(software_financial_help_cleaned, ',')
        ) as extracted_value

)

select
    answer_id,
    software_financial_help_item as software_financial_help
from exploded
where software_financial_help_item <> ''
