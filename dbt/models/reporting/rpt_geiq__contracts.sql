with contracts as (

    select *
    from {{ ref('fct_geiq__contracts') }}

),

assessments as (

    select *
    from {{ ref('fct_geiq__assessments') }}

),

geiq as (

    select *
    from {{ ref('dim_geiq') }}

),

departments as (

    select distinct
        code_departement_insee,
        nom_departement_complet,
        nom_region
    from {{ ref('dim_commune') }}

)

select
    {{ pilo_star(ref('fct_geiq__contracts'), relation_alias='contracts') }},

    assessments.state                   as assessment_state,
    assessments.is_final_reviewed       as is_assessment_final_reviewed,
    assessments.with_main_geiq,
    assessments.antenna_nb,
    assessments.employee_nb             as assessment_employee_nb,
    assessments.convention_amount,
    assessments.granted_amount,
    assessments.advance_amount,
    assessments.conventionned_institutions_departments,

    geiq.label_geiq_id,
    geiq.label_geiq_name,
    geiq.geiq_department,
    departments.nom_departement_complet as geiq_department_name,
    departments.nom_region              as geiq_region,
    geiq.antenna_department_nb,
    geiq.antenna_departments

from contracts
left join assessments on contracts.assessment_id = assessments.id
left join geiq
    on
        assessments.label_geiq_id = geiq.label_geiq_id
        and assessments.campaign_year = geiq.campaign_year
left join departments on geiq.geiq_department = departments.code_departement_insee
