with source as (

    select *
    from {{ source('esat_raw', 'surveys_esatanswer') }}

),

renamed as (

    select

        answer_ptr_id                        as answer_id,

        -- Identification
        finess_num,

        -- Organization
        esat_role,
        esat_name,
        esat_siret,
        managing_organization_finess,
        esat_status,
        esat_dept,
        nb_places_allowed,
        nb_employee_worked,
        nb_employee_shared,

        -- Workers supported
        nb_worker_acc                        as nb_worker_supported,
        nb_worker_half_time,
        mean_worker_age,
        mean_seniority,

        -- Workers new
        nb_worker_previous_mot,
        nb_worker_new,
        nb_worker_temporary,

        -- Establishment discovery
        nb_worker_mispe_mdph,
        nb_worker_mispe_rpe,

        -- Ordinary working environment
        nb_worker_willing_mot,
        nb_worker_ft_job_seekers,

        -- Ordinary working environment and customers involvement
        prescription_delegate                as has_prescription_delegate,
        pmsmp_refused                        as is_pmsmp_refused,
        nb_worker_pmsmp,
        nb_worker_service,
        nb_worker_mad_indiv,
        nb_worker_with_public,
        nb_worker_only_inside,
        nb_worker_cumul_esat_ea,
        nb_worker_cumul_esat_mot,

        -- Workers left
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

        -- Workers right to return
        nb_worker_reinteg,
        nb_worker_reinteg_other,
        nb_esat_agreement,

        -- Support hours
        nb_support_hours,
        support_themes,

        -- Formations
        contrib_opco                         as has_contrib_opco,
        pct_opco,
        nb_worker_formation_opco,
        opco_or_anfh_refusal                 as has_opco_or_anfh_refusal,
        nb_worker_cpf_unused                 as nb_worker_cpf_used,
        cpf_unused_reason,
        formation_cpf,
        nb_worker_intern_formation,
        formation_subject,
        autodetermination_formation          as has_autodetermination_formation,
        nb_worker_autodetermination,
        autodetermination_external_formation as has_autodetermination_external_formation,

        -- Skills
        skills_validation_type,
        nb_worker_rae_rsfp,
        nb_worker_vae,
        after_skills_validation,

        -- Duodays
        nb_worker_duoday,
        nb_employee_reverse_duoday,

        -- Skills notebook
        skills_notebook,
        software_financial_help,
        software_financial_help_type,

        -- Retirement
        retirement_preparation_actions,
        uaat_inscription,
        retirement_preparation_nb_workers,
        pct_more_than50,

        -- Language accessibility
        documents_falclist,

        -- Working conditions
        worker_delegate                      as has_worker_delegate,
        worker_delegate_formation,
        worker_delegate_hours_credit,
        nb_delegate_hours                    as has_delegate_hours,
        mix_qvt_in_place                     as has_mix_qvt_in_place,

        -- Profit sharing
        profit_sharing_bonus,
        mean_pct_esat_rem,

        -- Insurance policy
        foresight_in_place                   as has_foresight_in_place,
        year_foresight_in_place,

        -- Mobility program
        annual_transport_budget,
        nb_worker_transport,
        nb_worker_mobility_inclusion_card,
        nb_worker_driving_licence,
        nb_worker_code,

        -- Vouchers
        holiday_voucher                      as has_holiday_voucher,
        holiday_voucher_annual_budget,
        gift_voucher                         as has_gift_voucher,
        gift_voucher_annual_budget,

        -- Sunday work
        nb_worker_worked_sunday,

        -- Partnership agreements
        agreement_signed_ft                  as has_agreement_signed_ft,
        agreement_signed_ea                  as has_agreement_signed_ea,
        agreement_signed_dept_pae            as has_agreement_signed_dept_pae,

        -- Staff
        nb_insertion_staff,
        nb_insertion_dispo,
        insertion_staff_funding,

        -- Commercial operation
        annual_ca,
        annual_ca_production,
        annual_ca_service,
        annual_ca_mad,
        pct_ca_public,
        budget_commercial,
        budget_commercial_deficit,
        budget_commercial_excedent,

        -- Social activity budget
        budget_social,
        budget_social_deficit,
        budget_social_excedent,

        -- Investments
        budget_accessibility,
        budget_diversity,

        -- Comments
        comments

    from source

)

select *
from renamed
