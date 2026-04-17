with source as (

    select
        finess_num,
        support_themes
    from {{ ref('int_esat__surveys_esat_answers_deduplicated') }}
    where
        support_themes is not null
        and trim(support_themes) <> ''

),

cleaned as (

    select
        finess_num,
        trim(
            both '[]' from replace(support_themes, '''', '')
        ) as support_theme_list
    from source

),

exploded as (

    select
        finess_num,
        trim(extracted_value) as support_theme
    from cleaned,
        unnest(string_to_array(support_theme_list, ',')) as extracted_value

)

select
    finess_num,
    support_theme
from exploded
where support_theme <> ''
