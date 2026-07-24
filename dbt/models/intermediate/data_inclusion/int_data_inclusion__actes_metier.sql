with raw_actes as (

    select
        type_acte_raw,
        contexte_acte,
        reseaux_porteurs,
        to_char(date_trunc('month', mois), 'YYYY-MM') as mois,
        coalesce(raw_departement, 'Inconnu')          as raw_departement,
        sum(nombre_actes_metier)                      as nombre_actes
    from {{ ref('stg_actes_metier__data_inclusion') }}
    where
        mois >= date_trunc('month', current_date) - interval '14 months'
        and mois < date_trunc('month', current_date)
        and (
            contexte_acte is null
            or contexte_acte not in (
                'emplois-demo-widget',
                'emplois-pentest-widget',
                'les-emplois-demo-2026-01',
                'les-emplois-review-app-2026-01'
            )
        )
    group by
        to_char(date_trunc('month', mois), 'YYYY-MM'),
        type_acte_raw,
        contexte_acte,
        reseaux_porteurs,
        coalesce(raw_departement, 'Inconnu')

),

reseau_tokens as (

    select
        raw_actes.mois,
        raw_actes.type_acte_raw,
        raw_actes.contexte_acte,
        raw_actes.reseaux_porteurs,
        raw_actes.raw_departement,
        raw_actes.nombre_actes,
        tokens.ord,
        trim(both '"' from trim(tokens.reseau)) as reseau
    from raw_actes
    left join
        lateral unnest(
            string_to_array(trim(both '{}' from coalesce(raw_actes.reseaux_porteurs, '')), ',')
        ) with ordinality as tokens (reseau, ord)
        on raw_actes.type_acte_raw in ('mise à jour de structure', 'mise à jour de service')

),

mapped_reseaux as (

    select
        mois,
        type_acte_raw,
        contexte_acte,
        reseaux_porteurs,
        raw_departement,
        nombre_actes,
        ord,
        case reseau
            when 'france-travail' then 'FRANCE_TRAVAIL'
            when 'mission-locale' then 'MISSION_LOCALE'
            when 'cap-emploi-reseau-cheops' then 'CAP_EMPLOI'
            when 'departements' then 'CONSEIL_DEPARTEMENTAL'
            when 'maisons-des-solidarites' then 'CONSEIL_DEPARTEMENTAL'
            when 'maison-departementale-de-lautonomie' then 'CONSEIL_DEPARTEMENTAL'
            when 'ccas-cias' then 'CCAS_CIAS'
            when 'caf' then 'CAF_MSA'
            when 'residences-fjt' then 'CAF_MSA'
            when 'cnam' then 'CAF_MSA'
            when 'cpam' then 'CAF_MSA'
            when 'aci' then 'SIAE'
            when 'ei' then 'SIAE'
            when 'etti' then 'SIAE'
            when 'eiti' then 'SIAE'
            when 'esat' then 'SIAE'
            when 'geiq' then 'SIAE'
            when 'coorace' then 'SIAE'
            when 'unea' then 'SIAE'
            when 'chantier-ecole' then 'SIAE'
            when 'etcld' then 'SIAE'
            when 'inae' then 'SIAE'
            when 'agil-ess' then 'SIAE'
            when 'action-logement' then 'TS_HEBERGEMENT'
            when 'chrs' then 'TS_HEBERGEMENT'
            when 'chu' then 'TS_HEBERGEMENT'
            when 'cada' then 'TS_HEBERGEMENT'
            when 'cph' then 'TS_HEBERGEMENT'
            when 'siao' then 'TS_HEBERGEMENT'
            when 'federation-des-acteurs-de-la-solidarite' then 'TS_HEBERGEMENT'
            when 'afpa' then 'E2C_EPIDE_AFPA'
            when 'ecoles-de-la-deuxieme-chance' then 'E2C_EPIDE_AFPA'
            when 'reseau-app' then 'E2C_EPIDE_AFPA'
            when 'collectif-emploi' then 'E2C_EPIDE_AFPA'
            when 'reseau-bge' then 'E2C_EPIDE_AFPA'
            when 'agefiph' then 'TS_SPECIALISES'
            when 'cidff' then 'TS_SPECIALISES'
            when 'csapa' then 'TS_SPECIALISES'
            when 'cmp' then 'TS_SPECIALISES'
            when 'mdph' then 'TS_SPECIALISES'
            when 'croix-rouge' then 'TS_SPECIALISES'
            when 'restos-du-coeur' then 'TS_SPECIALISES'
            when 'secours-populaire' then 'TS_SPECIALISES'
            when 'plie' then 'PLIE'
            when 'alliance-villes-emploi' then 'PLIE'
            when 'maisons-de-l-emploi' then 'PLIE'
            when 'spip' then 'JUSTICE_PROBATION'
            when 'pjj' then 'JUSTICE_PROBATION'
            when 'points-justice' then 'JUSTICE_PROBATION'
            when 'mobin' then 'MOBILITE'
            when 'wimoov' then 'MOBILITE'
            when 'conseillers-numeriques' then 'ACC_NUMERIQUE'
            when 'aidants-connect' then 'ACC_NUMERIQUE'
            when 'mediation-numerique' then 'ACC_NUMERIQUE'
        end as mapped_type_structure
    from reseau_tokens

),

with_reseau_type as (

    select
        mois,
        type_acte_raw,
        contexte_acte,
        reseaux_porteurs,
        raw_departement,
        nombre_actes,
        coalesce(
            (array_agg(mapped_type_structure order by ord) filter (
                where mapped_type_structure is not null and mapped_type_structure != 'ACC_NUMERIQUE'
            ))[1],
            (array_agg(mapped_type_structure order by ord) filter (
                where mapped_type_structure = 'ACC_NUMERIQUE'
            ))[1],
            'Autre'
        ) as reseau_type_structure
    from mapped_reseaux
    group by
        mois,
        type_acte_raw,
        contexte_acte,
        reseaux_porteurs,
        raw_departement,
        nombre_actes

),

normalized as (

    select
        mois,
        'data-inclusion' as source,
        'Support'        as categorie_acte,
        false            as north_star,
        false            as north_star_70,
        false            as traite,
        raw_departement,
        nombre_actes,
        case type_acte_raw
            when 'recherche' then 'Recherche data·inclusion'
            when 'mise à jour de structure'
                then 'Mise à jour d’une structure d’offre de service, hors employeur solidaire'
            when 'mise à jour de service' then 'Mise à jour offre de service, hors emploi solidaire'
            else type_acte_raw
        end              as type_acte,
        case
            when type_acte_raw in ('mise à jour de structure', 'mise à jour de service') then reseau_type_structure
            when contexte_acte = 'france-travail' then 'FRANCE_TRAVAIL'
            when contexte_acte = 'mes-aides-france-travail' then 'FRANCE_TRAVAIL'
            when contexte_acte = 'pilotage-réunion-france-travail' then 'FRANCE_TRAVAIL'
            when contexte_acte = 'les-emplois' then 'SIAE'
            when contexte_acte = 'emplois-de-linclusion' then 'SIAE'
            when contexte_acte = 'cd35' then 'CONSEIL_DEPARTEMENTAL'
            when contexte_acte = 'cd80-widget' then 'CONSEIL_DEPARTEMENTAL'
            when contexte_acte = 'hautespyrenees-widget' then 'CONSEIL_DEPARTEMENTAL'
            when contexte_acte = 'worldline-parcoursrsa' then 'DELEGATAIRE_RSA'
            when contexte_acte = 'monenfant' then 'CAF_MSA'
            when contexte_acte = 'agefiph' then 'TS_SPECIALISES'
            when contexte_acte = 'finess' then 'TS_SPECIALISES'
            when contexte_acte = 'action-logement' then 'TS_HEBERGEMENT'
            when contexte_acte = 'alhpi-widget' then 'TS_HEBERGEMENT'
            when contexte_acte = 'association-entourage-widget' then 'TS_HEBERGEMENT'
            when contexte_acte = 'rezosocial.com' then 'TS_HEBERGEMENT'
            when contexte_acte = 'soliguide' then 'TS_HEBERGEMENT'
            when contexte_acte = 'cfppa-widget' then 'E2C_EPIDE_AFPA'
            when contexte_acte = 'ouvreboite-afpa-widget' then 'E2C_EPIDE_AFPA'
            when contexte_acte = 'cscendoume-widget' then 'CCAS_CIAS'
            when contexte_acte = 'mdemarseille-widget' then 'CCAS_CIAS'
            when contexte_acte = 'mon-suivi-social-widget' then 'CCAS_CIAS'
            when contexte_acte = 'pyramide-est-widget' then 'CCAS_CIAS'
            else 'Autre'
        end              as type_structure
    from with_reseau_type

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
    from normalized

),

with_departement as (

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
from with_departement
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
