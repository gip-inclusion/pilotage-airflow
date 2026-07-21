{% macro nullify_excluded_esat_survey_answer_fields(source_alias, field_exclusions_alias) %}
    {#
        Garde toutes les colonnes des réponses ESAT.

        Si la table des exclusions contient une ligne pour une réponse et une
        colonne, la valeur de cette colonne est remplacée par `null` pour cette
        réponse.

        La liste des colonnes est écrite ici pour que dbt puisse préparer le SQL
        sans se connecter à la base.

        Arguments:
        - source_alias: nom utilisé dans le SQL pour la table des réponses.
        - field_exclusions_alias: nom utilisé dans le SQL pour la table des exclusions.
    #}
    {% set columns = [
        'answer_id',
        'finess_num',
        'esat_role',
        'esat_name',
        'esat_siret',
        'managing_organization_finess',
        'esat_status',
        'esat_dept',
        'nb_places_allowed',
        'nb_employee_worked',
        'nb_employee_shared',
        'nb_worker_supported',
        'nb_worker_half_time',
        'mean_worker_age',
        'mean_seniority',
        'nb_worker_previous_mot',
        'nb_worker_new',
        'nb_worker_temporary',
        'nb_worker_mispe_mdph',
        'nb_worker_mispe_rpe',
        'nb_worker_willing_mot',
        'nb_worker_ft_job_seekers',
        'has_prescription_delegate',
        'is_pmsmp_refused',
        'nb_worker_pmsmp',
        'nb_worker_service',
        'nb_worker_mad_indiv',
        'nb_worker_with_public',
        'nb_worker_only_inside',
        'nb_worker_cumul_esat_ea',
        'nb_worker_cumul_esat_mot',
        'nb_worker_left',
        'nb_worker_left_ea',
        'nb_worker_left_private',
        'nb_worker_left_public',
        'nb_worker_left_asso',
        'nb_worker_left_other_reason',
        'nb_worker_cdi',
        'nb_worker_cdd',
        'nb_worker_interim',
        'nb_worker_prof',
        'nb_worker_apprentice',
        'nb_conv_exit_agreement',
        'nb_conv_exit_agreement_new',
        'nb_worker_esrp',
        'nb_worker_reinteg',
        'nb_worker_reinteg_other',
        'nb_esat_agreement',
        'nb_support_hours',
        'support_themes',
        'has_contrib_opco',
        'pct_opco',
        'nb_worker_formation_opco',
        'has_opco_or_anfh_refusal',
        'nb_worker_cpf_used',
        'cpf_unused_reason',
        'formation_cpf',
        'nb_worker_intern_formation',
        'formation_subject',
        'has_autodetermination_formation',
        'nb_worker_autodetermination',
        'has_autodetermination_external_formation',
        'skills_validation_type',
        'nb_worker_rae_rsfp',
        'nb_worker_vae',
        'after_skills_validation',
        'nb_worker_duoday',
        'nb_employee_reverse_duoday',
        'skills_notebook',
        'software_financial_help',
        'software_financial_help_type',
        'retirement_preparation_actions',
        'uaat_inscription',
        'retirement_preparation_nb_workers',
        'pct_more_than50',
        'documents_falclist',
        'has_worker_delegate',
        'worker_delegate_formation',
        'worker_delegate_hours_credit',
        'has_delegate_hours',
        'has_mix_qvt_in_place',
        'profit_sharing_bonus',
        'mean_pct_esat_rem',
        'has_foresight_in_place',
        'year_foresight_in_place',
        'annual_transport_budget',
        'nb_worker_transport',
        'nb_worker_mobility_inclusion_card',
        'nb_worker_driving_licence',
        'nb_worker_code',
        'has_holiday_voucher',
        'holiday_voucher_annual_budget',
        'has_gift_voucher',
        'gift_voucher_annual_budget',
        'nb_worker_worked_sunday',
        'has_agreement_signed_ft',
        'has_agreement_signed_ea',
        'has_agreement_signed_dept_pae',
        'nb_insertion_staff',
        'nb_insertion_dispo',
        'insertion_staff_funding',
        'annual_ca',
        'annual_ca_production',
        'annual_ca_service',
        'annual_ca_mad',
        'pct_ca_public',
        'budget_commercial',
        'budget_commercial_deficit',
        'budget_commercial_excedent',
        'budget_social',
        'budget_social_deficit',
        'budget_social_excedent',
        'budget_accessibility',
        'budget_diversity',
        'comments',
    ] %}

    {% for column_name in columns %}
    case
        when exists (
            select 1
            from {{ field_exclusions_alias }}
            where
                {{ field_exclusions_alias }}.answer_id = {{ source_alias }}.answer_id
                and {{ field_exclusions_alias }}.field_name = '{{ column_name }}'
        ) then null
        else {{ source_alias }}.{{ column_name }}
    end as {{ column_name }}{% if not loop.last %},{% endif %}
    {% endfor %}
{% endmacro %}
