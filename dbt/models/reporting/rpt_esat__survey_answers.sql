with esat_survey_answers_mapping as (

    select *
    from {{ ref('fct_esat__survey_answers_mapping') }}

),

esat as (

    select *
    from {{ ref('dim_esat') }}

),

communes as (

    select
        code_commune_insee,
        nom_commune,
        code_departement_insee,
        nom_departement,
        code_region_insee,
        nom_region
    from {{ ref('dim_commune') }}

),

survey_answers as (

    select *
    from {{ ref('esat__survey_answers_core') }}

)

select
    esat_survey_answers_mapping.finess_num,
    esat_survey_answers_mapping.answer_id,
    esat_survey_answers_mapping.matching_method,
    esat_survey_answers_mapping.matching_confidence,

    esat.esat_name,
    esat.adresse,
    esat.code_postal,
    esat.type_esat,
    esat.siret,
    esat.code_commune_insee,
    esat.capacite_autorisee,
    esat.managing_organization_finess,

    communes.nom_commune,
    communes.code_departement_insee,
    communes.nom_departement,
    communes.code_region_insee,
    communes.nom_region,

    survey_answers.finess_num                   as answer_finess_num,
    survey_answers.esat_name                    as answer_esat_name,
    survey_answers.esat_siret                   as answer_esat_siret,
    survey_answers.managing_organization_finess as answer_managing_organization_finess,

    {{ pilo_star(
        ref('esat__survey_answers_core'),
        except=[
            "answer_id",
            "finess_num",
            "esat_name",
            "esat_siret",
            "managing_organization_finess",
            "duplicate_group_esat_names",
            "duplicate_group_finess_nums"
        ],
        relation_alias='survey_answers'
    ) }}

from esat_survey_answers_mapping
left join esat
    on esat_survey_answers_mapping.finess_num = esat.finess_num
left join communes
    on esat.code_commune_insee = communes.code_commune_insee
left join survey_answers
    on esat_survey_answers_mapping.answer_id = survey_answers.answer_id
