with source as (

    select
        finess_num,
        skills_validation_type
    from {{ ref('int_esat__surveys_esat_answers_deduplicated') }}
    where
        skills_validation_type is not null
        and trim(skills_validation_type) <> ''

),

cleaned as (

    select
        finess_num,
        trim(
            both '[]' from replace(skills_validation_type, '''', '')
        ) as skills_validation_type_list
    from source

),

exploded as (

    select
        finess_num,
        trim(extracted_value) as skills_validation_type
    from cleaned,
        unnest(string_to_array(skills_validation_type_list, ',')) as extracted_value

)

select
    finess_num,
    skills_validation_type
from exploded
where skills_validation_type <> ''
