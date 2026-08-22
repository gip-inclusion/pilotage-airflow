with recursive commandes as (

    select
        commandes."Type de structure"                                          as raw_type_structure,
        commandes."Nom Departement"                                            as raw_departement,
        to_char(date_trunc('month', commandes."Date d'expédition"), 'YYYY-MM') as mois_expedition,
        sum(cast(commandes."Nombre de Carnets" as integer))                    as carnets
    from {{ ref('Commandes') }} as commandes
    where
        commandes."Date d'expédition" is not null
        and commandes."Date d'expédition" >= '2024-01-01'
        and commandes."Date d'expédition" < date_trunc('month', current_date)
    group by
        to_char(date_trunc('month', commandes."Date d'expédition"), 'YYYY-MM'),
        commandes."Type de structure",
        commandes."Nom Departement"

),

normalized_commandes as (

    select
        mois_expedition,
        carnets,
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
        end as type_structure,
        case
            when raw_departement is null or trim(cast(raw_departement as text)) = '' then 'Inconnu'
            when upper(split_part(trim(cast(raw_departement as text)), ' - ', 1)) in ('2A', '2B')
                then upper(split_part(trim(cast(raw_departement as text)), ' - ', 1))
            when
                split_part(trim(cast(raw_departement as text)), ' - ', 1) ~ '^[0-9]+$'
                and cast(split_part(trim(cast(raw_departement as text)), ' - ', 1) as integer) between 1 and 95
                then lpad(cast(cast(split_part(trim(cast(raw_departement as text)), ' - ', 1) as integer) as text), 2, '0')
            when
                split_part(trim(cast(raw_departement as text)), ' - ', 1) ~ '^[0-9]+$'
                and cast(split_part(trim(cast(raw_departement as text)), ' - ', 1) as integer) between 971 and 976
                then cast(cast(split_part(trim(cast(raw_departement as text)), ' - ', 1) as integer) as text)
            else 'Inconnu'
        end as departement
    from commandes

),

grouped_commandes as (

    select
        mois_expedition,
        type_structure,
        departement,
        sum(carnets) as carnets
    from normalized_commandes
    group by
        mois_expedition,
        type_structure,
        departement

),

distribution as (

    select
        to_char(
            to_date(grouped_commandes.mois_expedition || '-01', 'YYYY-MM-DD')
            + ((months_after.value - 1) * interval '1 month'),
            'YYYY-MM'
        )                                                                                    as mois,
        grouped_commandes.mois_expedition,
        'monrecap'                                                                           as source,
        'Distribution carnet Mon Récap'                                                      as type_acte,
        'Accompagnement'                                                                     as categorie_acte,
        false                                                                                as north_star,
        false                                                                                as north_star_70,
        false                                                                                as traite,
        grouped_commandes.type_structure,
        grouped_commandes.departement,
        cast(grouped_commandes.carnets as double precision) * cast(0.10 as double precision) as nombre_actes
    from grouped_commandes
    cross join generate_series(1, 10) as months_after (value)

),

remplissage as (

    select
        to_char(
            to_date(grouped_commandes.mois_expedition || '-01', 'YYYY-MM-DD')
            + ((months_after.value - 1) * interval '1 month'),
            'YYYY-MM'
        )                                 as mois,
        grouped_commandes.mois_expedition,
        'monrecap'                        as source,
        'Remplissage carnet Mon Récap'    as type_acte,
        'Support'                         as categorie_acte,
        false                             as north_star,
        false                             as north_star_70,
        false                             as traite,
        grouped_commandes.type_structure,
        grouped_commandes.departement,
        cast(grouped_commandes.carnets as double precision)
        * (
            least(cast(months_after.value as double precision) / 10, 1)
            - least(cast(greatest(months_after.value - 6, 0) as double precision) / 10, 1)
        )
        * cast(1.093 as double precision) as nombre_actes
    from grouped_commandes
    cross join generate_series(1, 6) as months_after (value)

),

actes as (

    select *
    from distribution

    union all

    select *
    from remplissage

),

ordered_actes as (

    select
        mois,
        mois_expedition,
        source,
        type_acte,
        categorie_acte,
        north_star,
        north_star_70,
        traite,
        type_structure,
        departement,
        nombre_actes,
        row_number() over (
            partition by
                mois,
                source,
                type_acte,
                categorie_acte,
                north_star,
                north_star_70,
                traite,
                type_structure,
                departement
            order by mois_expedition
        ) as contribution_rank
    from actes
    where
        to_date(mois || '-01', 'YYYY-MM-DD') >= date_trunc('month', current_date) - interval '14 months'
        and to_date(mois || '-01', 'YYYY-MM-DD') < date_trunc('month', current_date)

),

running_totals as (

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
        contribution_rank,
        nombre_actes as running_nombre_actes
    from ordered_actes
    where contribution_rank = 1

    union all

    select
        ordered_actes.mois,
        ordered_actes.source,
        ordered_actes.type_acte,
        ordered_actes.categorie_acte,
        ordered_actes.north_star,
        ordered_actes.north_star_70,
        ordered_actes.traite,
        ordered_actes.type_structure,
        ordered_actes.departement,
        ordered_actes.contribution_rank,
        running_totals.running_nombre_actes + ordered_actes.nombre_actes as running_nombre_actes
    from running_totals
    inner join ordered_actes
        on
            running_totals.mois = ordered_actes.mois
            and running_totals.source = ordered_actes.source
            and running_totals.type_acte = ordered_actes.type_acte
            and running_totals.categorie_acte = ordered_actes.categorie_acte
            and running_totals.north_star = ordered_actes.north_star
            and running_totals.north_star_70 = ordered_actes.north_star_70
            and running_totals.traite = ordered_actes.traite
            and running_totals.type_structure = ordered_actes.type_structure
            and running_totals.departement = ordered_actes.departement
            and running_totals.contribution_rank + 1 = ordered_actes.contribution_rank

),

aggregated as (

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
        running_nombre_actes as nombre_actes
    from running_totals
    where not exists (
        select 1
        from running_totals as next_contribution
        where
            running_totals.mois = next_contribution.mois
            and running_totals.source = next_contribution.source
            and running_totals.type_acte = next_contribution.type_acte
            and running_totals.categorie_acte = next_contribution.categorie_acte
            and running_totals.north_star = next_contribution.north_star
            and running_totals.north_star_70 = next_contribution.north_star_70
            and running_totals.traite = next_contribution.traite
            and running_totals.type_structure = next_contribution.type_structure
            and running_totals.departement = next_contribution.departement
            and running_totals.contribution_rank + 1 = next_contribution.contribution_rank
    )

),

rounded as (

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
        cast(round(nombre_actes) as integer) as nombre_actes
    from aggregated

)

select *
from rounded
where nombre_actes > 0
