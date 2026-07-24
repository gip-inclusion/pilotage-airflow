with raw_actes as (

    select
        to_char(date_trunc('month', candidatures.date_candidature), 'YYYY-MM')                                                   as mois,
        'emplois'                                                                                                                as source,
        'candidature_employeur_solidaire'                                                                                        as type_acte_code,
        'Accompagnement'                                                                                                         as categorie_acte,
        coalesce(candidatures."état" not in ('Nouvelle candidature', 'Candidature en attente', 'Candidature à l''étude'), false)
        and candidatures.date_traitement is not null
        and candidatures.date_traitement - candidatures.date_candidature between 0 and 30
            as north_star,
        coalesce(candidatures."état" not in ('Nouvelle candidature', 'Candidature en attente', 'Candidature à l''étude'), false)
        and candidatures.date_traitement is not null
        and candidatures.date_traitement - candidatures.date_candidature between 0 and 70
            as north_star_70,
        coalesce(candidatures."état" not in ('Nouvelle candidature', 'Candidature en attente', 'Candidature à l''étude'), false)
            as traite,
        candidatures.type_org_prescripteur                                                                                       as raw_type_structure,
        coalesce(candidatures.dept_org, 'Inconnu')                                                                               as raw_departement,
        count(*)                                                                                                                 as nombre_actes
    from {{ ref('candidatures_echelle_locale') }} as candidatures
    where
        candidatures.injection_ai = 0
        and candidatures.date_candidature >= date_trunc('month', current_date) - interval '14 months'
        and candidatures.date_candidature < date_trunc('month', current_date)
    group by
        to_char(date_trunc('month', candidatures.date_candidature), 'YYYY-MM'),
        coalesce(candidatures."état" not in ('Nouvelle candidature', 'Candidature en attente', 'Candidature à l''étude'), false)
        and candidatures.date_traitement is not null
        and candidatures.date_traitement - candidatures.date_candidature between 0 and 30,
        coalesce(candidatures."état" not in ('Nouvelle candidature', 'Candidature en attente', 'Candidature à l''étude'), false)
        and candidatures.date_traitement is not null
        and candidatures.date_traitement - candidatures.date_candidature between 0 and 70,
        coalesce(candidatures."état" not in ('Nouvelle candidature', 'Candidature en attente', 'Candidature à l''étude'), false),
        candidatures.type_org_prescripteur,
        coalesce(candidatures.dept_org, 'Inconnu')

    union all

    select
        to_char(date_trunc('month', fiches.date_creation), 'YYYY-MM') as mois,
        'emplois'                                                     as source,
        'creation_offre_emploi'                                       as type_acte_code,
        'Support'                                                     as categorie_acte,
        true                                                          as north_star,
        true                                                          as north_star_70,
        true                                                          as traite,
        fiches.type_employeur                                         as raw_type_structure,
        coalesce(fiches.departement_employeur, 'Inconnu')             as raw_departement,
        count(*)                                                      as nombre_actes
    from {{ ref('stg_emplois__fiches_de_poste') }} as fiches
    where
        fiches.date_creation >= date_trunc('month', current_date) - interval '14 months'
        and fiches.date_creation < date_trunc('month', current_date)
    group by
        to_char(date_trunc('month', fiches.date_creation), 'YYYY-MM'),
        fiches.type_employeur,
        coalesce(fiches.departement_employeur, 'Inconnu')

    union all

    select
        to_char(date_trunc('month', fiches.date_derniere_modification), 'YYYY-MM') as mois,
        'emplois'                                                                  as source,
        'mise_a_jour_offre_emploi'                                                 as type_acte_code,
        'Support'                                                                  as categorie_acte,
        true                                                                       as north_star,
        true                                                                       as north_star_70,
        true                                                                       as traite,
        fiches.type_employeur                                                      as raw_type_structure,
        coalesce(fiches.departement_employeur, 'Inconnu')                          as raw_departement,
        count(*)                                                                   as nombre_actes
    from {{ ref('stg_emplois__fiches_de_poste') }} as fiches
    where
        fiches.date_derniere_modification > fiches.date_creation
        and fiches.date_derniere_modification >= date_trunc('month', current_date) - interval '14 months'
        and fiches.date_derniere_modification < date_trunc('month', current_date)
    group by
        to_char(date_trunc('month', fiches.date_derniere_modification), 'YYYY-MM'),
        fiches.type_employeur,
        coalesce(fiches.departement_employeur, 'Inconnu')

    union all

    select
        to_char(date_trunc('month', structures.date_inscription), 'YYYY-MM') as mois,
        'emplois'                                                            as source,
        'creation_employeur_solidaire'                                       as type_acte_code,
        'Support'                                                            as categorie_acte,
        false                                                                as north_star,
        false                                                                as north_star_70,
        false                                                                as traite,
        'SIAE'                                                               as raw_type_structure,
        coalesce(structures."département", 'Inconnu')                        as raw_departement,
        count(*)                                                             as nombre_actes
    from {{ ref('structures') }} as structures
    where
        structures.date_inscription >= date_trunc('month', current_date) - interval '14 months'
        and structures.date_inscription < date_trunc('month', current_date)
    group by
        to_char(date_trunc('month', structures.date_inscription), 'YYYY-MM'),
        coalesce(structures."département", 'Inconnu')

    union all

    select
        to_char(date_trunc('month', candidats.date_diagnostic), 'YYYY-MM')                            as mois,
        'emplois'                                                                                     as source,
        'diagnostic_iae'                                                                              as type_acte_code,
        'Accompagnement'                                                                              as categorie_acte,
        false                                                                                         as north_star,
        false                                                                                         as north_star_70,
        false                                                                                         as traite,
        regexp_replace(trim(candidats.sous_type_auteur_diagnostic), '^[^[:space:]]+[[:space:]]+', '')
            as raw_type_structure,
        coalesce(candidats."département_diag", 'Inconnu')                                             as raw_departement,
        count(distinct candidats.id)                                                                  as nombre_actes
    from {{ ref('candidats') }} as candidats
    where
        candidats.date_diagnostic >= date_trunc('month', current_date) - interval '14 months'
        and candidats.date_diagnostic < date_trunc('month', current_date)
    group by
        to_char(date_trunc('month', candidats.date_diagnostic), 'YYYY-MM'),
        regexp_replace(trim(candidats.sous_type_auteur_diagnostic), '^[^[:space:]]+[[:space:]]+', ''),
        coalesce(candidats."département_diag", 'Inconnu')

),

normalized_type_structure as (

    select
        mois,
        source,
        type_acte_code,
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
        type_acte_code,
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
        type_acte_code,
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

),

labeled as (

    select
        mois,
        source,
        categorie_acte,
        north_star,
        north_star_70,
        traite,
        type_structure,
        departement,
        nombre_actes,
        case type_acte_code
            when 'candidature_employeur_solidaire' then 'Candidature auprès d’un employeur solidaire'
            when 'creation_offre_emploi' then 'Création offre d’emploi'
            when 'mise_a_jour_offre_emploi' then 'Mise à jour offre d’emploi'
            when 'creation_employeur_solidaire' then 'Création employeur solidaire'
            when 'diagnostic_iae' then 'Diagnostic IAE'
        end as type_acte
    from normalized

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
from labeled
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
