with esat_survey_answers_mapping as (

    select *
    from {{ ref('fct_esat__survey_answers_mapping') }}

),

esat as (

    select
        finess_num,
        code_commune_insee
    from {{ ref('dim_esat') }}

),

communes as (

    select
        code_commune_insee,
        code_region_insee,
        nom_region
    from {{ ref('dim_commune') }}

),

answers_by_region as (

    select
        communes.code_region_insee,
        communes.nom_region,

        case
            when esat_survey_answers_mapping.matching_confidence = 'high' then 'answered'
            when esat_survey_answers_mapping.matching_confidence = 'low' then 'pending'
            when esat_survey_answers_mapping.matching_confidence = 'no_answer' then 'no_answer'
        end as answer_status

    from esat_survey_answers_mapping
    left join esat
        on esat_survey_answers_mapping.finess_num = esat.finess_num
    left join communes
        on esat.code_commune_insee = communes.code_commune_insee
    where communes.code_region_insee is not null

),

aggregated as (

    select
        code_region_insee,
        nom_region,

        count(*) as total_esat,

        count(*) filter (
            where answer_status = 'answered'
        )        as answered_count,

        count(*) filter (
            where answer_status = 'pending'
        )        as pending_count,

        count(*) filter (
            where answer_status = 'no_answer'
        )        as no_answer_count

    from answers_by_region
    group by
        code_region_insee,
        nom_region

)

select
    code_region_insee,
    nom_region,

    total_esat,

    answered_count,
    pending_count,
    no_answer_count,

    round(
        100.0 * answered_count / nullif(total_esat, 0),
        2
    ) as answered_rate,

    round(
        100.0 * pending_count / nullif(total_esat, 0),
        2
    ) as pending_rate,

    round(
        100.0 * no_answer_count / nullif(total_esat, 0),
        2
    ) as no_answer_rate

from aggregated
order by nom_region
