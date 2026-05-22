with low_confidence_esat as (

    select finess_num
    from {{ ref('fct_esat__survey_answers_mapping') }}
    where matching_confidence = 'low'

),

esat as (

    select
        finess_num,
        managing_organization_finess
    from {{ ref('dim_esat') }}

),

survey_answers as (

    select *
    from {{ ref('esat__survey_answers_core') }}

),

departements as (

    select distinct
        code_departement_insee,
        nom_departement,
        code_region_insee,
        nom_region
    from {{ ref('dim_commune') }}

),

potential_answers as (

    select distinct survey_answers.answer_id
    from low_confidence_esat
    inner join esat
        on low_confidence_esat.finess_num = esat.finess_num
    inner join survey_answers
        on
            esat.managing_organization_finess
            = survey_answers.managing_organization_finess
            or esat.managing_organization_finess
            = any(survey_answers.duplicate_group_finess_nums)

)

select
    {{ pilo_star(
        ref('esat__survey_answers_core'),
        except=[
            "duplicate_group_finess_nums",
            "duplicate_group_esat_names"
        ],
        relation_alias='survey_answers'
    ) }},

    departements.code_departement_insee,
    departements.nom_departement,
    departements.code_region_insee,
    departements.nom_region

from potential_answers
inner join survey_answers
    on potential_answers.answer_id = survey_answers.answer_id
left join departements
    on survey_answers.esat_dept = departements.code_departement_insee
