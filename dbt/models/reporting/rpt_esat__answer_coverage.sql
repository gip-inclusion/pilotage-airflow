with esat as (

    select *
    from {{ ref('dim_esat') }}

),

survey_answers as (

    select *
    from {{ ref('esat__survey_answers_core') }}

),

survey_answer_managing_organizations as (

    select distinct managing_organization_finess
    from survey_answers
    where managing_organization_finess is not null

)

select
    esat.establishment_finess_num,
    esat.establishment_name,
    esat.establishment_type,
    esat.managing_organization_finess,

    direct_answer.finess_num is not null                                          as has_direct_survey_answer_finess,

    managing_organization_answer.finess_num is not null
        as has_managing_organization_survey_answer,

    same_name_answer.esat_name is not null
        as has_direct_survey_answer_same_name,

    same_managing_organization_answer.managing_organization_finess is not null
        as has_same_managing_organization_survey_answer,

    direct_answer.finess_num is not null
    or same_name_answer.esat_name is not null
        as has_direct_answer_coverage,

    direct_answer.finess_num is not null
    or managing_organization_answer.finess_num is not null
    or same_name_answer.esat_name is not null
    or same_managing_organization_answer.managing_organization_finess is not null
        as has_answer_coverage

from esat

left join survey_answers as direct_answer
    on esat.establishment_finess_num = direct_answer.finess_num

left join survey_answers as managing_organization_answer
    on esat.managing_organization_finess = managing_organization_answer.finess_num

left join survey_answers as same_name_answer
    on
        lower(regexp_replace(trim(esat.establishment_name), '\s+', ' ', 'g'))
        = lower(regexp_replace(trim(same_name_answer.esat_name), '\s+', ' ', 'g'))

left join survey_answer_managing_organizations as same_managing_organization_answer
    on
        esat.managing_organization_finess
        = same_managing_organization_answer.managing_organization_finess
