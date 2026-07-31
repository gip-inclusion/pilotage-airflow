with raw_actes as (

    select
        'GPS'                                                       as source,
        'Support'                                                   as categorie_acte,
        false                                                       as north_star,
        false                                                       as north_star_70,
        false                                                       as traite,
        gps_logs.type_org                                           as raw_type_structure,
        to_char(date_trunc('month', gps_logs.timestamp), 'YYYY-MM') as mois,
        case
            when gps_logs.view_name in (
                'gps:group_memberships',
                'gps:group_beneficiary',
                'gps:display_contact_info',
                'gps:old_group_list'
            ) then 'Consultation groupe de suivi'
            else 'Mise à jour groupe de suivi'
        end                                                         as type_acte,
        coalesce(gps_logs.dept_org, 'Inconnu')                      as raw_departement,
        count(*)                                                    as nombre_actes
    from {{ ref('gps_logs_users') }} as gps_logs
    where
        gps_logs.group_id is not null
        and gps_logs.view_name != 'gps:group_list'
        and gps_logs.timestamp >= date_trunc('month', current_date) - interval '14 months'
        and gps_logs.timestamp < date_trunc('month', current_date)
    group by
        to_char(date_trunc('month', gps_logs.timestamp), 'YYYY-MM'),
        case
            when gps_logs.view_name in (
                'gps:group_memberships',
                'gps:group_beneficiary',
                'gps:display_contact_info',
                'gps:old_group_list'
            ) then 'Consultation groupe de suivi'
            else 'Mise à jour groupe de suivi'
        end,
        gps_logs.type_org,
        coalesce(gps_logs.dept_org, 'Inconnu')

),

normalized_type_structure as (

    select
        mois,
        source,
        type_acte,
        categorie_acte,
        north_star,
        north_star_70,
        traite,
        raw_departement,
        nombre_actes,
        case
            when raw_type_structure is null or trim(cast(raw_type_structure as text)) = '' then 'Autre'
            when raw_type_structure in ('FT', 'PE', 'france_travail') then 'FRANCE_TRAVAIL'
            when raw_type_structure in ('ML', 'mission_locale') then 'MISSION_LOCALE'
            when raw_type_structure in ('CAP_EMPLOI', 'cap_emploi') then 'CAP_EMPLOI'
            when raw_type_structure in ('DEPT', 'CD', 'conseil_departemental') then 'CONSEIL_DEPARTEMENTAL'
            when raw_type_structure in ('ODC', 'delegataire_rsa', 'DELEGATAIRE_RSA') then 'DELEGATAIRE_RSA'
            when raw_type_structure in ('CCAS', 'CIAS', 'ASE') then 'CCAS_CIAS'
            when raw_type_structure in ('SPIP', 'PJJ', 'JUSTICE') then 'JUSTICE_PROBATION'
            when raw_type_structure in (
                'CHU', 'CHRS', 'CPH', 'CADA', 'HUDA', 'RS_FJT', 'OIL', 'PENSION', 'ACT', 'LHSS',
                'CHRS/Accueil de jour', 'Accueil de jour'
            ) then 'TS_HEBERGEMENT'
            when raw_type_structure in (
                'EI', 'AI', 'ACI', 'ETTI', 'EITI', 'GEIQ', 'EA', 'EATT', 'OPCS', 'siae', 'employer',
                'Groupements d’employeurs pour l’insertion et la qualification', 'SIAE'
            ) then 'SIAE'
            when raw_type_structure = 'PLIE' then 'PLIE'
            when raw_type_structure in ('E2C', 'EPIDE', 'AFPA', 'OF', 'Organisme de formation') then 'E2C_EPIDE_AFPA'
            when raw_type_structure in ('CIDFF', 'CSAPA', 'CAARUD', 'PREVENTION', 'OHPD', 'OCASF') then 'TS_SPECIALISES'
            when raw_type_structure in ('CAF', 'MSA') then 'CAF_MSA'
            when raw_type_structure in ('PIJ_BIJ', 'OACAS', 'CAVA') then 'AUTRES_INSERTION'
            when raw_type_structure in ('BUYER', 'PARTNER', 'INDIVIDUAL', 'ADMIN') then 'Autre'
            else 'Autre'
        end as type_structure
    from raw_actes

),

department_tokens as (

    select
        mois,
        source,
        type_acte,
        categorie_acte,
        north_star,
        north_star_70,
        traite,
        type_structure,
        raw_departement,
        nombre_actes,
        upper(split_part(trim(cast(raw_departement as text)), ' - ', 1)) as departement_token
    from normalized_type_structure

),

normalized as (

    select
        mois,
        source,
        type_acte,
        categorie_acte,
        north_star,
        north_star_70,
        traite,
        type_structure,
        nombre_actes,
        case
            when raw_departement is null or trim(cast(raw_departement as text)) = '' then 'Inconnu'
            when departement_token in ('2A', '2B') then departement_token
            when
                departement_token ~ '^[0-9]+$'
                and cast(departement_token as integer) between 1 and 95
                then lpad(cast(cast(departement_token as integer) as text), 2, '0')
            when
                departement_token ~ '^[0-9]+$'
                and cast(departement_token as integer) between 971 and 976
                then cast(cast(departement_token as integer) as text)
            else 'Inconnu'
        end as departement
    from department_tokens

)

select
    mois,
    source,
    type_acte,
    categorie_acte,
    north_star,
    north_star_70,
    traite,
    type_structure,
    departement,
    cast(sum(nombre_actes) as integer) as nombre_actes
from normalized
where nombre_actes > 0
group by
    mois,
    source,
    type_acte,
    categorie_acte,
    north_star,
    north_star_70,
    traite,
    type_structure,
    departement
