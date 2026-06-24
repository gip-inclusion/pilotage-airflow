with mapped_answers as (

    select
        answer_id,
        finess_num
    from {{ ref('fct_esat__survey_answers_mapping') }}
    where answer_id is not null

),

mapped_answers_by_answer as (

    select
        answer_id,
        count(distinct finess_num)                         as mapped_esat_count,
        array_agg(distinct finess_num order by finess_num) as mapped_finess_nums
    from mapped_answers
    group by answer_id

),

staging_answers as (

    select *
    from {{ ref('stg_esat__surveys_esat_answers') }}

),

deduplicated_answers as (

    select
        answer_id,
        duplicate_group_finess_nums,
        duplicate_group_esat_names,
        completeness_score
    from {{ ref('int_esat__surveys_esat_answers_deduplicated') }}

),

final as (

    select
        staging_answers.answer_id,
        staging_answers.finess_num,

        deduplicated_answers.duplicate_group_finess_nums,

        staging_answers.esat_role,
        staging_answers.esat_name,

        deduplicated_answers.duplicate_group_esat_names,
        deduplicated_answers.completeness_score,

        staging_answers.esat_siret,
        staging_answers.managing_organization_finess,
        staging_answers.esat_status,
        staging_answers.esat_dept,
        staging_answers.nb_places_allowed,
        staging_answers.nb_employee_worked,
        staging_answers.nb_employee_shared,
        staging_answers.nb_worker_supported,
        staging_answers.nb_worker_half_time,
        staging_answers.mean_worker_age,
        staging_answers.mean_seniority,
        staging_answers.nb_worker_previous_mot,
        staging_answers.nb_worker_new,
        staging_answers.nb_worker_temporary,
        staging_answers.nb_worker_mispe_mdph,
        staging_answers.nb_worker_mispe_rpe,
        staging_answers.nb_worker_willing_mot,
        staging_answers.nb_worker_ft_job_seekers,
        staging_answers.has_prescription_delegate,
        staging_answers.is_pmsmp_refused,
        staging_answers.nb_worker_pmsmp,
        staging_answers.nb_worker_service,
        staging_answers.nb_worker_mad_indiv,
        staging_answers.nb_worker_with_public,
        staging_answers.nb_worker_only_inside,
        staging_answers.nb_worker_cumul_esat_ea,
        staging_answers.nb_worker_cumul_esat_mot,
        staging_answers.nb_worker_left,
        staging_answers.nb_worker_left_ea,
        staging_answers.nb_worker_left_private,
        staging_answers.nb_worker_left_public,
        staging_answers.nb_worker_left_asso,
        staging_answers.nb_worker_left_other_reason,
        staging_answers.nb_worker_cdi,
        staging_answers.nb_worker_cdd,
        staging_answers.nb_worker_interim,
        staging_answers.nb_worker_prof,
        staging_answers.nb_worker_apprentice,
        staging_answers.nb_conv_exit_agreement,
        staging_answers.nb_conv_exit_agreement_new,
        staging_answers.nb_worker_esrp,
        staging_answers.nb_worker_reinteg,
        staging_answers.nb_worker_reinteg_other,
        staging_answers.nb_esat_agreement,
        staging_answers.nb_support_hours,
        staging_answers.support_themes,
        staging_answers.has_contrib_opco,
        staging_answers.pct_opco,
        staging_answers.nb_worker_formation_opco,
        staging_answers.has_opco_or_anfh_refusal,
        staging_answers.nb_worker_cpf_used,
        staging_answers.cpf_unused_reason,
        staging_answers.formation_cpf,
        staging_answers.nb_worker_intern_formation,
        staging_answers.formation_subject,
        staging_answers.has_autodetermination_formation,
        staging_answers.nb_worker_autodetermination,
        staging_answers.has_autodetermination_external_formation,
        staging_answers.skills_validation_type,
        staging_answers.nb_worker_rae_rsfp,
        staging_answers.nb_worker_vae,
        staging_answers.after_skills_validation,
        staging_answers.nb_worker_duoday,
        staging_answers.nb_employee_reverse_duoday,
        staging_answers.skills_notebook,
        staging_answers.software_financial_help,
        staging_answers.software_financial_help_type,
        staging_answers.retirement_preparation_actions,
        staging_answers.uaat_inscription,
        staging_answers.retirement_preparation_nb_workers,
        staging_answers.pct_more_than50,
        staging_answers.documents_falclist,
        staging_answers.has_worker_delegate,
        staging_answers.worker_delegate_formation,
        staging_answers.worker_delegate_hours_credit,
        staging_answers.has_delegate_hours,
        staging_answers.has_mix_qvt_in_place,
        staging_answers.profit_sharing_bonus,
        staging_answers.mean_pct_esat_rem,
        staging_answers.has_foresight_in_place,
        staging_answers.year_foresight_in_place,
        staging_answers.annual_transport_budget,
        staging_answers.nb_worker_transport,
        staging_answers.nb_worker_mobility_inclusion_card,
        staging_answers.nb_worker_driving_licence,
        staging_answers.nb_worker_code,
        staging_answers.has_holiday_voucher,
        staging_answers.holiday_voucher_annual_budget,
        staging_answers.has_gift_voucher,
        staging_answers.gift_voucher_annual_budget,
        staging_answers.nb_worker_worked_sunday,
        staging_answers.has_agreement_signed_ft,
        staging_answers.has_agreement_signed_ea,
        staging_answers.has_agreement_signed_dept_pae,
        staging_answers.nb_insertion_staff,
        staging_answers.nb_insertion_dispo,
        staging_answers.insertion_staff_funding,
        staging_answers.annual_ca,
        staging_answers.annual_ca_production,
        staging_answers.annual_ca_service,
        staging_answers.annual_ca_mad,
        staging_answers.pct_ca_public,
        staging_answers.budget_commercial,
        staging_answers.budget_commercial_deficit,
        staging_answers.budget_commercial_excedent,
        staging_answers.budget_social,
        staging_answers.budget_social_deficit,
        staging_answers.budget_social_excedent,
        staging_answers.budget_accessibility,
        staging_answers.budget_diversity,
        staging_answers.comments,

        mapped_answers_by_answer.mapped_esat_count,
        mapped_answers_by_answer.mapped_finess_nums

    from staging_answers
    inner join mapped_answers_by_answer
        on staging_answers.answer_id = mapped_answers_by_answer.answer_id
    left join deduplicated_answers
        on staging_answers.answer_id = deduplicated_answers.answer_id

)

select *
from final
