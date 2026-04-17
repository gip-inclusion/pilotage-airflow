with source as (

    select
        finess_num,
        software_financial_help
    from {{ ref('int_esat__surveys_esat_answers_deduplicated') }}
    where software_financial_help is not null

),

cleaned as (

    select
        finess_num,

        trim(
            both '[]' from replace(software_financial_help, '''', '')
        ) as software_financial_help_cleaned
    from source
    where trim(software_financial_help) <> ''

),

exploded as (

    select
        finess_num,
        trim(extracted_value) as software_financial_help_item
    from cleaned,
        unnest(
            string_to_array(software_financial_help_cleaned, ',')
        ) as extracted_value

)

select
    finess_num,
    software_financial_help_item as software_financial_help
from exploded
where software_financial_help_item <> ''
