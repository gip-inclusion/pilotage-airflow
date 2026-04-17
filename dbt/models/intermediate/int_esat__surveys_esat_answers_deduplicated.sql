with base as (

    select *
    from {{ ref('stg_esat__surveys_esat_answers') }}
    where finess_num is not null

),

scored as (

    select
        base.*,

        (
            {{ completeness_case('esat_role') }}
            + {{ completeness_case('esat_name') }}
            + {{ completeness_case('esat_siret') }}
            + {{ completeness_case('managing_organization_finess') }}
            + {{ completeness_case('esat_status') }}
            + {{ completeness_case('esat_dept') }}
            + {{ completeness_case('nb_places_allowed') }}
            + {{ completeness_case('nb_employee_worked') }}
            + {{ completeness_case('nb_employee_shared') }}
            + {{ completeness_case('nb_worker_supported') }}
            + {{ completeness_case('nb_worker_half_time') }}
            + {{ completeness_case('mean_worker_age') }}
            + {{ completeness_case('mean_seniority') }}
            + {{ completeness_case('nb_worker_previous_mot') }}
            + {{ completeness_case('nb_worker_new') }}
            + {{ completeness_case('nb_worker_temporary') }}
            + {{ completeness_case('nb_worker_mispe_mdph') }}
            + {{ completeness_case('nb_worker_mispe_rpe') }}
            + {{ completeness_case('nb_worker_willing_mot') }}
            + {{ completeness_case('nb_worker_ft_job_seekers') }}
            + {{ completeness_case('has_prescription_delegate') }}
            + {{ completeness_case('is_pmsmp_refused') }}
            + {{ completeness_case('nb_worker_pmsmp') }}
            + {{ completeness_case('nb_worker_service') }}
            + {{ completeness_case('nb_worker_mad_indiv') }}
            + {{ completeness_case('nb_worker_with_public') }}
            + {{ completeness_case('nb_worker_only_inside') }}
            + {{ completeness_case('nb_worker_cumul_esat_ea') }}
            + {{ completeness_case('nb_worker_cumul_esat_mot') }}
            + {{ completeness_case('nb_worker_left') }}
            + {{ completeness_case('nb_worker_left_ea') }}
            + {{ completeness_case('nb_worker_left_private') }}
            + {{ completeness_case('nb_worker_left_public') }}
            + {{ completeness_case('nb_worker_left_asso') }}
            + {{ completeness_case('nb_worker_left_other_reason') }}
            + {{ completeness_case('nb_worker_cdi') }}
            + {{ completeness_case('nb_worker_cdd') }}
            + {{ completeness_case('nb_worker_interim') }}
            + {{ completeness_case('nb_worker_prof') }}
            + {{ completeness_case('nb_worker_apprentice') }}
            + {{ completeness_case('nb_conv_exit_agreement') }}
            + {{ completeness_case('nb_conv_exit_agreement_new') }}
            + {{ completeness_case('nb_worker_esrp') }}
            + {{ completeness_case('nb_worker_reinteg') }}
            + {{ completeness_case('nb_worker_reinteg_other') }}
            + {{ completeness_case('nb_esat_agreement') }}
            + {{ completeness_case('nb_support_hours') }}
            + {{ completeness_case('support_themes') }}
            + {{ completeness_case('has_contrib_opco') }}
            + {{ completeness_case('pct_opco') }}
            + {{ completeness_case('nb_worker_formation_opco') }}
            + {{ completeness_case('has_opco_or_anfh_refusal') }}
            + {{ completeness_case('nb_worker_cpf_used') }}
            + {{ completeness_case('cpf_unused_reason') }}
            + {{ completeness_case('formation_cpf') }}
            + {{ completeness_case('nb_worker_intern_formation') }}
            + {{ completeness_case('formation_subject') }}
            + {{ completeness_case('has_autodetermination_formation') }}
            + {{ completeness_case('nb_worker_autodetermination') }}
            + {{ completeness_case('has_autodetermination_external_formation') }}
            + {{ completeness_case('skills_validation_type') }}
            + {{ completeness_case('nb_worker_rae_rsfp') }}
            + {{ completeness_case('nb_worker_vae') }}
            + {{ completeness_case('after_skills_validation') }}
            + {{ completeness_case('nb_worker_duoday') }}
            + {{ completeness_case('nb_employee_reverse_duoday') }}
            + {{ completeness_case('skills_notebook') }}
            + {{ completeness_case('software_financial_help') }}
            + {{ completeness_case('software_financial_help_type') }}
            + {{ completeness_case('retirement_preparation_actions') }}
            + {{ completeness_case('uaat_inscription') }}
            + {{ completeness_case('retirement_preparation_nb_workers') }}
            + {{ completeness_case('pct_more_than50') }}
            + {{ completeness_case('documents_falclist') }}
            + {{ completeness_case('has_worker_delegate') }}
            + {{ completeness_case('worker_delegate_formation') }}
            + {{ completeness_case('worker_delegate_hours_credit') }}
            + {{ completeness_case('has_delegate_hours') }}
            + {{ completeness_case('has_mix_qvt_in_place') }}
            + {{ completeness_case('profit_sharing_bonus') }}
            + {{ completeness_case('mean_pct_esat_rem') }}
            + {{ completeness_case('has_foresight_in_place') }}
            + {{ completeness_case('year_foresight_in_place') }}
            + {{ completeness_case('annual_transport_budget') }}
            + {{ completeness_case('nb_worker_transport') }}
            + {{ completeness_case('nb_worker_mobility_inclusion_card') }}
            + {{ completeness_case('nb_worker_driving_licence') }}
            + {{ completeness_case('nb_worker_code') }}
            + {{ completeness_case('has_holiday_voucher') }}
            + {{ completeness_case('holiday_voucher_annual_budget') }}
            + {{ completeness_case('has_gift_voucher') }}
            + {{ completeness_case('gift_voucher_annual_budget') }}
            + {{ completeness_case('nb_worker_worked_sunday') }}
            + {{ completeness_case('has_agreement_signed_ft') }}
            + {{ completeness_case('has_agreement_signed_ea') }}
            + {{ completeness_case('has_agreement_signed_dept_pae') }}
            + {{ completeness_case('nb_insertion_staff') }}
            + {{ completeness_case('nb_insertion_dispo') }}
            + {{ completeness_case('insertion_staff_funding') }}
            + {{ completeness_case('annual_ca') }}
            + {{ completeness_case('annual_ca_production') }}
            + {{ completeness_case('annual_ca_service') }}
            + {{ completeness_case('annual_ca_mad') }}
            + {{ completeness_case('pct_ca_public') }}
            + {{ completeness_case('budget_commercial') }}
            + {{ completeness_case('budget_commercial_deficit') }}
            + {{ completeness_case('budget_commercial_excedent') }}
            + {{ completeness_case('budget_social') }}
            + {{ completeness_case('budget_social_deficit') }}
            + {{ completeness_case('budget_social_excedent') }}
            + {{ completeness_case('budget_accessibility') }}
            + {{ completeness_case('budget_diversity') }}
            + {{ completeness_case('comments') }}
        ) as completeness_score

    from base

),

ranked as (

    select
        *,
        row_number() over (
            partition by finess_num
            order by completeness_score desc, answer_id desc
        ) as rn
    from scored

)

select
    answer_id,
    finess_num,
    esat_role,
    esat_name,
    esat_siret,
    managing_organization_finess,
    esat_status,
    esat_dept,
    nb_places_allowed,
    nb_employee_worked,
    nb_employee_shared,
    nb_worker_supported,
    nb_worker_half_time,
    mean_worker_age,
    mean_seniority,
    nb_worker_previous_mot,
    nb_worker_new,
    nb_worker_temporary,
    nb_worker_mispe_mdph,
    nb_worker_mispe_rpe,
    nb_worker_willing_mot,
    nb_worker_ft_job_seekers,
    has_prescription_delegate,
    is_pmsmp_refused,
    nb_worker_pmsmp,
    nb_worker_service,
    nb_worker_mad_indiv,
    nb_worker_with_public,
    nb_worker_only_inside,
    nb_worker_cumul_esat_ea,
    nb_worker_cumul_esat_mot,
    nb_worker_left,
    nb_worker_left_ea,
    nb_worker_left_private,
    nb_worker_left_public,
    nb_worker_left_asso,
    nb_worker_left_other_reason,
    nb_worker_cdi,
    nb_worker_cdd,
    nb_worker_interim,
    nb_worker_prof,
    nb_worker_apprentice,
    nb_conv_exit_agreement,
    nb_conv_exit_agreement_new,
    nb_worker_esrp,
    nb_worker_reinteg,
    nb_worker_reinteg_other,
    nb_esat_agreement,
    nb_support_hours,
    support_themes,
    has_contrib_opco,
    pct_opco,
    nb_worker_formation_opco,
    has_opco_or_anfh_refusal,
    nb_worker_cpf_used,
    cpf_unused_reason,
    formation_cpf,
    nb_worker_intern_formation,
    formation_subject,
    has_autodetermination_formation,
    nb_worker_autodetermination,
    has_autodetermination_external_formation,
    skills_validation_type,
    nb_worker_rae_rsfp,
    nb_worker_vae,
    after_skills_validation,
    nb_worker_duoday,
    nb_employee_reverse_duoday,
    skills_notebook,
    software_financial_help,
    software_financial_help_type,
    retirement_preparation_actions,
    uaat_inscription,
    retirement_preparation_nb_workers,
    pct_more_than50,
    documents_falclist,
    has_worker_delegate,
    worker_delegate_formation,
    worker_delegate_hours_credit,
    has_delegate_hours,
    has_mix_qvt_in_place,
    profit_sharing_bonus,
    mean_pct_esat_rem,
    has_foresight_in_place,
    year_foresight_in_place,
    annual_transport_budget,
    nb_worker_transport,
    nb_worker_mobility_inclusion_card,
    nb_worker_driving_licence,
    nb_worker_code,
    has_holiday_voucher,
    holiday_voucher_annual_budget,
    has_gift_voucher,
    gift_voucher_annual_budget,
    nb_worker_worked_sunday,
    has_agreement_signed_ft,
    has_agreement_signed_ea,
    has_agreement_signed_dept_pae,
    nb_insertion_staff,
    nb_insertion_dispo,
    insertion_staff_funding,
    annual_ca,
    annual_ca_production,
    annual_ca_service,
    annual_ca_mad,
    pct_ca_public,
    budget_commercial,
    budget_commercial_deficit,
    budget_commercial_excedent,
    budget_social,
    budget_social_deficit,
    budget_social_excedent,
    budget_accessibility,
    budget_diversity,
    comments,
    completeness_score
from ranked
where rn = 1
