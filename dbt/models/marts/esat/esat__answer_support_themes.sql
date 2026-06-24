with source as (

    select
        answer_id,
        support_themes
    from {{ ref('fct_esat__survey_answers') }}
    where
        support_themes is not null
        and trim(support_themes) <> ''

),

cleaned as (

    select
        answer_id,
        trim(
            both '[]' from replace(support_themes, '''', '')
        ) as support_theme_list
    from source

),

exploded as (

    select
        answer_id,
        trim(extracted_value) as support_theme
    from cleaned,
        unnest(
            string_to_array(support_theme_list, ',')
        ) as extracted_value

)

select
    answer_id,
    support_theme
from exploded
where support_theme <> ''
