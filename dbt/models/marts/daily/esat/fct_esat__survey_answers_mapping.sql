with esat as (

    select
        finess_num,
        managing_organization_finess,
        lower(trim(esat_name)) as esat_name
    from {{ ref('dim_esat') }}

),

survey_answers as (

    select
        answer_id,
        duplicate_group_finess_nums,
        managing_organization_finess,
        lower(trim(esat_name)) as esat_name
    from {{ ref('esat__survey_answers_core') }}

),

mapped_answers as (

    select
        finess_num,
        answer_id
    from {{ ref('stg_esat__mapped_answers') }}

),

candidate_matches as (

    select
        esat.finess_num,
        mapped_answers.answer_id,
        1                as matching_priority,
        'manual_mapping' as matching_method,
        'high'           as matching_confidence
    from esat
    inner join mapped_answers
        on esat.finess_num = mapped_answers.finess_num

    union all

    select
        esat.finess_num,
        survey_answers.answer_id,
        2               as matching_priority,
        'direct_finess' as matching_method,
        'high'          as matching_confidence
    from esat
    inner join survey_answers
        on esat.finess_num = any(survey_answers.duplicate_group_finess_nums)

    union all

    select
        esat.finess_num,
        survey_answers.answer_id,
        3                                             as matching_priority,
        'same_name_and_related_managing_organization' as matching_method,
        'high'                                        as matching_confidence
    from esat
    inner join survey_answers
        on esat.esat_name = survey_answers.esat_name
    where
        esat.managing_organization_finess = survey_answers.managing_organization_finess
        or esat.managing_organization_finess
        = any(survey_answers.duplicate_group_finess_nums)

    union all

    select
        esat.finess_num,
        survey_answers.answer_id,
        4                               as matching_priority,
        'related_managing_organization' as matching_method,
        'low'                           as matching_confidence
    from esat
    inner join survey_answers
        on
            esat.managing_organization_finess
            = survey_answers.managing_organization_finess
            or esat.managing_organization_finess
            = any(survey_answers.duplicate_group_finess_nums)

),

ranked_matches as (

    select
        *,
        row_number() over (
            partition by finess_num
            order by matching_priority, answer_id
        ) as matching_rank
    from candidate_matches

),

best_matches as (

    select
        finess_num,
        answer_id,
        matching_method,
        matching_confidence
    from ranked_matches
    where matching_rank = 1

)

select
    esat.finess_num,

    case
        when best_matches.matching_confidence = 'low' then null
        else best_matches.answer_id
    end                                                     as answer_id,

    coalesce(best_matches.matching_method, 'no_answer')     as matching_method,

    coalesce(best_matches.matching_confidence, 'no_answer') as matching_confidence

from esat
left join best_matches
    on esat.finess_num = best_matches.finess_num
