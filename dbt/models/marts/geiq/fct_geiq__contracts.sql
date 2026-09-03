select
    id,
    assessment_id,
    employee_id,
    campaign_year,
    antenna_department,
    start_at,
    planned_end_at,
    end_at,
    nb_days_in_campaign_year,
    allowance_amount,
    allowance_request_justification_reason,
    allowance_refusal_reason,
    date_mise_à_jour_metabase,
    end_at - start_at                        as real_contract_duration,
    planned_end_at - start_at                as theoretical_contract_duration,
    allowance_requested = 1                  as is_allowance_requested,
    allowance_granted = 1                    as is_allowance_granted,
    allowance_granted_previous_year = 1      as is_allowance_granted_previous_year,
    allowance_requested = 1
    and nb_days_in_campaign_year < 90
    and allowance_request_justification_reason
    = 'Prise en compte de l''accompagnement' as is_short_contract_with_derogation
from {{ ref('stg_geiq__contracts') }}
