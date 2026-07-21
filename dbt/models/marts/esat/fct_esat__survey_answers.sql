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

all_answers as (

    select *
    from {{ ref('int_esat__surveys_esat_answers_cleaned') }}

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
        all_answers.answer_id,
        all_answers.finess_num,

        deduplicated_answers.duplicate_group_finess_nums,

        all_answers.esat_role,
        all_answers.esat_name,

        deduplicated_answers.duplicate_group_esat_names,
        deduplicated_answers.completeness_score,

        all_answers.esat_siret,
        all_answers.managing_organization_finess,
        all_answers.esat_status,
        all_answers.esat_dept,
        all_answers.nb_places_allowed,
        all_answers.nb_employee_worked,
        all_answers.nb_employee_shared,
        all_answers.nb_worker_supported,
        all_answers.nb_worker_half_time,
        all_answers.mean_worker_age,
        all_answers.mean_seniority,
        all_answers.nb_worker_previous_mot,
        all_answers.nb_worker_new,
        all_answers.nb_worker_temporary,
        all_answers.nb_worker_mispe_mdph,
        all_answers.nb_worker_mispe_rpe,
        all_answers.nb_worker_willing_mot,
        all_answers.nb_worker_ft_job_seekers,
        all_answers.has_prescription_delegate,
        all_answers.is_pmsmp_refused,
        all_answers.nb_worker_pmsmp,
        all_answers.nb_worker_service,
        all_answers.nb_worker_mad_indiv,
        all_answers.nb_worker_with_public,
        all_answers.nb_worker_only_inside,
        all_answers.nb_worker_cumul_esat_ea,
        all_answers.nb_worker_cumul_esat_mot,
        all_answers.nb_worker_left,
        all_answers.nb_worker_left_ea,
        all_answers.nb_worker_left_private,
        all_answers.nb_worker_left_public,
        all_answers.nb_worker_left_asso,
        all_answers.nb_worker_left_other_reason,
        all_answers.nb_worker_cdi,
        all_answers.nb_worker_cdd,
        all_answers.nb_worker_interim,
        all_answers.nb_worker_prof,
        all_answers.nb_worker_apprentice,
        all_answers.nb_conv_exit_agreement,
        all_answers.nb_conv_exit_agreement_new,
        all_answers.nb_worker_esrp,
        all_answers.nb_worker_reinteg,
        all_answers.nb_worker_reinteg_other,
        all_answers.nb_esat_agreement,
        all_answers.nb_support_hours,
        all_answers.support_themes,
        all_answers.has_contrib_opco,
        all_answers.pct_opco,
        all_answers.nb_worker_formation_opco,
        all_answers.has_opco_or_anfh_refusal,
        all_answers.nb_worker_cpf_used,
        all_answers.cpf_unused_reason,
        all_answers.formation_cpf,
        all_answers.nb_worker_intern_formation,
        all_answers.formation_subject,
        all_answers.has_autodetermination_formation,
        all_answers.nb_worker_autodetermination,
        all_answers.has_autodetermination_external_formation,
        all_answers.skills_validation_type,
        all_answers.nb_worker_rae_rsfp,
        all_answers.nb_worker_vae,
        all_answers.after_skills_validation,
        all_answers.nb_worker_duoday,
        all_answers.nb_employee_reverse_duoday,
        all_answers.skills_notebook,
        all_answers.software_financial_help,
        all_answers.software_financial_help_type,
        all_answers.retirement_preparation_actions,
        all_answers.uaat_inscription,
        all_answers.retirement_preparation_nb_workers,
        all_answers.pct_more_than50,
        all_answers.documents_falclist,
        all_answers.has_worker_delegate,
        all_answers.worker_delegate_formation,
        all_answers.worker_delegate_hours_credit,
        all_answers.has_delegate_hours,
        all_answers.has_mix_qvt_in_place,
        all_answers.profit_sharing_bonus,
        all_answers.mean_pct_esat_rem,
        all_answers.has_foresight_in_place,
        all_answers.year_foresight_in_place,
        all_answers.annual_transport_budget,
        all_answers.nb_worker_transport,
        all_answers.nb_worker_mobility_inclusion_card,
        all_answers.nb_worker_driving_licence,
        all_answers.nb_worker_code,
        all_answers.has_holiday_voucher,
        all_answers.holiday_voucher_annual_budget,
        all_answers.has_gift_voucher,
        all_answers.gift_voucher_annual_budget,
        all_answers.nb_worker_worked_sunday,
        all_answers.has_agreement_signed_ft,
        all_answers.has_agreement_signed_ea,
        all_answers.has_agreement_signed_dept_pae,
        all_answers.nb_insertion_staff,
        all_answers.nb_insertion_dispo,
        all_answers.insertion_staff_funding,
        all_answers.annual_ca,
        all_answers.annual_ca_production,
        all_answers.annual_ca_service,
        all_answers.annual_ca_mad,
        all_answers.pct_ca_public,
        all_answers.budget_commercial,
        all_answers.budget_commercial_deficit,
        all_answers.budget_commercial_excedent,
        all_answers.budget_social,
        all_answers.budget_social_deficit,
        all_answers.budget_social_excedent,
        all_answers.budget_accessibility,
        all_answers.budget_diversity,
        all_answers.comments,

        mapped_answers_by_answer.mapped_esat_count,
        mapped_answers_by_answer.mapped_finess_nums

    from all_answers
    inner join mapped_answers_by_answer
        on all_answers.answer_id = mapped_answers_by_answer.answer_id
    left join deduplicated_answers
        on all_answers.answer_id = deduplicated_answers.answer_id

)

select *
from final
