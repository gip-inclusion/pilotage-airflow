with source as (

    select *
    from {{ ref('stg_esat__surveys_esat_answers') }}

),

field_exclusions as (

    select *
    from {{ ref('stg_esat__answer_field_exclusions') }}

)

select
    {{ nullify_excluded_esat_survey_answer_fields(
        source_alias='source',
        field_exclusions_alias='field_exclusions'
    ) }}
from source
