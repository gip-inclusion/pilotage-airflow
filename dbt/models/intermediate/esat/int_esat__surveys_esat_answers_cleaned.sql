with source as (

    select *
    from {{ ref('stg_esat__surveys_esat_answers') }}

),

field_exclusions as (

    select *
    from {{ ref('stg_esat__answer_field_exclusions') }}

),

excluded_answers as (

    select
        {{ nullify_excluded_esat_survey_answer_fields(
            source_alias='source',
            field_exclusions_alias='field_exclusions'
        ) }}
    from source

)

select
    excluded_answers.answer_id,
    excluded_answers.finess_num,
    excluded_answers.esat_role,
    excluded_answers.esat_name,
    excluded_answers.esat_siret,
    excluded_answers.managing_organization_finess,
    excluded_answers.esat_status,
    excluded_answers.esat_dept,
    excluded_answers.nb_places_allowed,
    excluded_answers.nb_employee_worked,
    excluded_answers.nb_employee_shared,
    excluded_answers.nb_worker_supported,
    excluded_answers.nb_worker_half_time,
    excluded_answers.mean_worker_age,
    excluded_answers.mean_seniority,
    excluded_answers.nb_worker_previous_mot,
    excluded_answers.nb_worker_new,
    excluded_answers.nb_worker_temporary,
    excluded_answers.nb_worker_mispe_mdph,
    excluded_answers.nb_worker_mispe_rpe,
    excluded_answers.nb_worker_willing_mot,
    excluded_answers.nb_worker_ft_job_seekers,
    excluded_answers.has_prescription_delegate,
    excluded_answers.is_pmsmp_refused,
    excluded_answers.nb_worker_pmsmp,
    excluded_answers.nb_worker_service,
    excluded_answers.nb_worker_mad_indiv,
    excluded_answers.nb_worker_with_public,
    excluded_answers.nb_worker_only_inside,
    excluded_answers.nb_worker_cumul_esat_ea,
    excluded_answers.nb_worker_cumul_esat_mot,
    excluded_answers.nb_worker_left,
    excluded_answers.nb_worker_left_ea,
    excluded_answers.nb_worker_left_private,
    excluded_answers.nb_worker_left_public,
    excluded_answers.nb_worker_left_asso,
    excluded_answers.nb_worker_left_other_reason,
    excluded_answers.nb_worker_cdi,
    excluded_answers.nb_worker_cdd,
    excluded_answers.nb_worker_interim,
    excluded_answers.nb_worker_prof,
    excluded_answers.nb_worker_apprentice,
    excluded_answers.nb_conv_exit_agreement,
    excluded_answers.nb_conv_exit_agreement_new,
    excluded_answers.nb_worker_esrp,
    excluded_answers.nb_worker_reinteg,
    excluded_answers.nb_worker_reinteg_other,
    excluded_answers.nb_esat_agreement,
    excluded_answers.nb_support_hours,
    excluded_answers.support_themes,
    excluded_answers.has_contrib_opco,
    excluded_answers.pct_opco,
    excluded_answers.nb_worker_formation_opco,
    excluded_answers.has_opco_or_anfh_refusal,
    excluded_answers.nb_worker_cpf_used,
    excluded_answers.cpf_unused_reason,
    excluded_answers.formation_cpf,
    excluded_answers.nb_worker_intern_formation,
    excluded_answers.formation_subject,
    excluded_answers.has_autodetermination_formation,
    excluded_answers.nb_worker_autodetermination,
    excluded_answers.has_autodetermination_external_formation,
    excluded_answers.skills_validation_type,
    excluded_answers.nb_worker_rae_rsfp,
    excluded_answers.nb_worker_vae,
    excluded_answers.after_skills_validation,
    excluded_answers.nb_worker_duoday,
    excluded_answers.nb_employee_reverse_duoday,
    excluded_answers.skills_notebook,
    excluded_answers.software_financial_help,
    excluded_answers.software_financial_help_type,
    excluded_answers.retirement_preparation_actions,
    excluded_answers.uaat_inscription,
    excluded_answers.retirement_preparation_nb_workers,
    excluded_answers.nb_worker_more_than_50,
    excluded_answers.documents_falclist,
    excluded_answers.has_worker_delegate,
    excluded_answers.worker_delegate_formation,
    excluded_answers.worker_delegate_hours_credit,
    excluded_answers.has_delegate_hours,
    excluded_answers.has_mix_qvt_in_place,
    excluded_answers.profit_sharing_bonus,
    excluded_answers.mean_pct_esat_rem,
    excluded_answers.has_foresight_in_place,
    excluded_answers.annual_transport_budget,
    excluded_answers.nb_worker_transport,
    excluded_answers.nb_worker_mobility_inclusion_card,
    excluded_answers.nb_worker_driving_licence,
    excluded_answers.nb_worker_code,
    excluded_answers.has_holiday_voucher,
    excluded_answers.holiday_voucher_annual_budget,
    excluded_answers.has_gift_voucher,
    excluded_answers.gift_voucher_annual_budget,
    excluded_answers.nb_worker_worked_sunday,
    excluded_answers.has_agreement_signed_ft,
    excluded_answers.has_agreement_signed_ea,
    excluded_answers.has_agreement_signed_dept_pae,
    excluded_answers.nb_insertion_staff,
    excluded_answers.nb_insertion_dispo,
    excluded_answers.insertion_staff_funding,
    excluded_answers.annual_ca,
    excluded_answers.annual_ca_production,
    excluded_answers.annual_ca_service,
    excluded_answers.annual_ca_mad,
    excluded_answers.pct_ca_public,
    excluded_answers.budget_commercial,
    excluded_answers.budget_commercial_deficit,
    excluded_answers.budget_commercial_excedent,
    excluded_answers.budget_social,
    excluded_answers.budget_social_deficit,
    excluded_answers.budget_social_excedent,
    excluded_answers.budget_accessibility,
    excluded_answers.budget_diversity,
    excluded_answers.comments,
    case
        when
            excluded_answers.has_foresight_in_place is true
            and excluded_answers.year_foresight_in_place between 1 and 2025
            then excluded_answers.year_foresight_in_place
    end as year_foresight_in_place
from excluded_answers
