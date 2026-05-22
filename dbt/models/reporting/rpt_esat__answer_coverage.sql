-- eventually this model should not count answers that has less than 10 completed columns (as per discussion with Alice)
with esat as (

    select *
    from {{ ref('dim_esat') }}

),

survey_answers as (

    select *
    from {{ ref('esat__survey_answers_core') }}

),

survey_answer_managing_organizations as (

    select distinct managing_organization_finess::text as managing_organization_finess
    from survey_answers
    where nullif(trim(managing_organization_finess::text), '') is not null

),

flags as (

    select
        esat.finess_num,
        esat.esat_name,
        esat.type_esat,
        esat.managing_organization_finess,

        exists(
            select 1
            from survey_answers
            where
                trim(esat.finess_num::text)
                = any(survey_answers.duplicate_group_finess_nums::text [])
        ) as has_direct_survey_answer_finess,

        exists(
            select 1
            from survey_answers
            where
                trim(esat.managing_organization_finess::text)
                = any(survey_answers.duplicate_group_finess_nums::text [])
        ) as has_indirect_survey_answer_from_managing_organization_finess,

        exists(
            select 1
            from survey_answers
            where
                exists (
                    select 1
                    from unnest(survey_answers.duplicate_group_esat_names) as esat_name_values (esat_name)
                    where
                        lower(
                            regexp_replace(
                                trim(esat.esat_name),
                                '\s+',
                                ' ',
                                'g'
                            )
                        )
                        =
                        lower(
                            regexp_replace(
                                trim(esat_name_values.esat_name),
                                '\s+',
                                ' ',
                                'g'
                            )
                        )
                )
                and (
                    trim(esat.managing_organization_finess::text)
                    = any(survey_answers.duplicate_group_finess_nums::text [])

                    or trim(esat.managing_organization_finess::text)
                    = trim(survey_answers.managing_organization_finess::text)
                )
        ) as has_indirect_survey_answer_matching_name_and_related_managing_organization,

        exists(
            select 1
            from survey_answer_managing_organizations
            where
                trim(esat.managing_organization_finess::text)
                = survey_answer_managing_organizations.managing_organization_finess
        ) as has_indirect_survey_answer_from_same_managing_organization

    from esat

),

final as (

    select
        finess_num,
        esat_name,
        type_esat,
        managing_organization_finess,

        has_direct_survey_answer_finess,
        has_indirect_survey_answer_from_managing_organization_finess,
        has_indirect_survey_answer_matching_name_and_related_managing_organization,
        has_indirect_survey_answer_from_same_managing_organization,

        has_direct_survey_answer_finess
        or has_indirect_survey_answer_from_managing_organization_finess
        or has_indirect_survey_answer_matching_name_and_related_managing_organization
        or has_indirect_survey_answer_from_same_managing_organization
            as has_answer_coverage

    from flags

)

select *
from final
