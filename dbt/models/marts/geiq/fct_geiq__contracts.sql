with contracts as (

    select *
    from {{ ref('int_geiq__contracts') }}

),

assessments as (

    select
        id,
        state,
        final_reviewed_at,
        label_geiq_id,
        label_geiq_name,
        department,
        department_name,
        region
    from {{ ref('int_geiq__assessments') }}

)

select
    contracts.id,
    contracts.assessment_id,
    contracts.employee_id,
    contracts.campaign_year,
    contracts.start_at,
    contracts.planned_end_at,
    contracts.end_at,
    contracts.real_contract_duration,
    contracts.theoretical_contract_duration,
    contracts.nb_days_in_campaign_year,
    contracts.allowance_amount,
    contracts.allowance_request_justification_reason,
    contracts.allowance_refusal_reason,
    contracts.is_allowance_requested,
    contracts.is_allowance_granted,
    contracts.is_allowance_granted_previous_year,
    contracts.date_mise_à_jour_metabase,
    assessments.label_geiq_id,
    assessments.label_geiq_name,
    contracts.department                     as antenna_department,
    contracts.department_name                as antenna_department_name,
    contracts.region                         as antenna_region,
    assessments.department                   as geiq_department,
    assessments.department_name              as geiq_department_name,
    assessments.region                       as geiq_region,
    contracts.is_allowance_requested
    and contracts.nb_days_in_campaign_year < 90
    and contracts.allowance_request_justification_reason
    = 'Prise en compte de l''accompagnement' as is_short_contract_with_derogation
from contracts
left join assessments on contracts.assessment_id = assessments.id
