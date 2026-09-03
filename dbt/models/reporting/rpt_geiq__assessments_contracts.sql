with assessments as (

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

),

contracts as (

    select
        assessment_id,
        count(*)                                                    as contract_nb,
        count(*) filter (where is_allowance_requested)              as requested_contract_nb,
        count(*) filter (where is_allowance_granted)                as granted_contract_nb,
        count(*) filter (where is_short_contract_with_derogation)   as short_derogated_contract_nb,
        sum(allowance_amount) filter (where is_allowance_requested) as requested_allowance_amount,
        sum(allowance_amount) filter (where is_allowance_granted)   as granted_allowance_amount
    from {{ ref('fct_geiq__contracts') }}
    group by assessment_id

)

select
    {{ pilo_star(ref('fct_geiq__assessments'), relation_alias='assessments') }},

    geiq.geiq_department,
    departments.nom_departement_complet                                              as geiq_department_name,
    departments.nom_region                                                           as geiq_region,

    coalesce(contracts.contract_nb, 0)                                               as contract_nb,
    coalesce(contracts.requested_contract_nb, 0)                                     as requested_contract_nb,
    coalesce(contracts.granted_contract_nb, 0)                                       as granted_contract_nb,
    coalesce(contracts.short_derogated_contract_nb, 0)                               as short_derogated_contract_nb,
    coalesce(contracts.requested_allowance_amount, 0)                                as requested_allowance_amount,
    coalesce(contracts.granted_allowance_amount, 0)                                  as granted_allowance_amount,
    coalesce(contracts.granted_allowance_amount, 0) - assessments.convention_amount
        as convention_vs_granted_allowance_gap,
    {{ safe_divide('assessments.granted_amount', 'assessments.convention_amount') }}
        as realization_rate

from assessments
left join geiq
    on
        assessments.label_geiq_id = geiq.label_geiq_id
        and assessments.campaign_year = geiq.campaign_year
left join departments on geiq.geiq_department = departments.code_departement_insee
left join contracts on assessments.id = contracts.assessment_id
